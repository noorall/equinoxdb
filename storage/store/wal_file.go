package store

import (
	"bytes"
	"encoding/binary"
	"equinox/internel"
	"equinox/pkg/pool"
	"equinox/storage/errs"
	"equinox/storage/types"
	"fmt"
	"github.com/golang/snappy"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	WALFileExtension = "wal"

	float64EntryType  = 1
	integerEntryType  = 2
	booleanEntryType  = 3
	stringEntryType   = 4
	unsignedEntryType = 5
)

type WalEntryType byte

const (
	// WriteWALEntryType indicates a write entry.
	WriteWALEntryType WalEntryType = 0x01

	// DeleteWALEntryType indicates a delete entry.
	DeleteWALEntryType WalEntryType = 0x02

	// DeleteRangeWALEntryType indicates a delete range entry.
	DeleteRangeWALEntryType WalEntryType = 0x03
)

var (
	bytesPool     = pool.NewBytes(256)
	bytesSyncPool = pool.NewBytesSyncPool()
)

type walker func(e *WALEntry) error

type WALEntry interface {
	Type() WalEntryType
	Encode(dst []byte) ([]byte, error)
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(b []byte) error
	MarshalSize() int
}

type WriteWALEntry struct {
	Values map[string][]types.Value
	sz     int
}

func (w *WriteWALEntry) MarshalSize() int {
	if w.sz > 0 || len(w.Values) == 0 {
		return w.sz
	}

	encLen := 7 * len(w.Values) // Type (1), Key Length (2), and Count (4) for each key

	// determine required length
	for k, v := range w.Values {
		encLen += len(k)
		if len(v) == 0 {
			return 0
		}

		encLen += 8 * len(v) // timestamps (8)

		switch v[0].(type) {
		case types.FloatValue, types.IntegerValue, types.UnsignedValue:
			encLen += 8 * len(v)
		case types.BooleanValue:
			encLen += 1 * len(v)
		case types.StringValue:
			for _, vv := range v {
				str, ok := vv.(types.StringValue)
				if !ok {
					return 0
				}
				encLen += 4 + len(str.RawValue())
			}
		default:
			return 0
		}
	}

	w.sz = encLen

	return w.sz
}

// Encode converts the WriteWALEntry into a byte stream using dst if it
// is large enough.  If dst is too small, the slice will be grown to fit the
// encoded entry.
func (w *WriteWALEntry) Encode(dst []byte) ([]byte, error) {
	// The entries values are encode as follows:
	//
	// For each key and slice of values, first a 1 byte types for the []types.Values
	// slice is written.  Following the types, the length and key bytes are written.
	// Following the key, a 4 byte count followed by each value as a 8 byte time
	// and N byte value.  The value is dependent on the types being encoded.  float64,
	// int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
	// except that string values have a 4-byte length, and keys only use 2 bytes.
	//
	// This structure is then repeated for each key an value slices.
	//
	// ┌────────────────────────────────────────────────────────────────────┐
	// │                           WriteWALEntry                            │
	// ├──────┬─────────┬────────┬───────┬─────────┬─────────┬───┬──────┬───┤
	// │ Type │ Key Len │   Key  │ Count │  Time   │  Value  │...│ Type │...│
	// │1 byte│ 2 bytes │ N bytes│4 bytes│ 8 bytes │ N bytes │   │1 byte│   │
	// └──────┴─────────┴────────┴───────┴─────────┴─────────┴───┴──────┴───┘

	encLen := w.MarshalSize() // Type (1), Key Length (2), and Count (4) for each key

	// allocate or re-slice to correct size
	if len(dst) < encLen {
		dst = make([]byte, encLen)
	} else {
		dst = dst[:encLen]
	}

	// Finally, encode the entry
	var n int
	var curType byte

	for k, v := range w.Values {
		switch v[0].(type) {
		case types.FloatValue:
			curType = float64EntryType
		case types.IntegerValue:
			curType = integerEntryType
		case types.UnsignedValue:
			curType = unsignedEntryType
		case types.BooleanValue:
			curType = booleanEntryType
		case types.StringValue:
			curType = stringEntryType
		default:
			return nil, fmt.Errorf("unsupported value types: %T", v[0])
		}
		dst[n] = curType
		n++

		binary.BigEndian.PutUint16(dst[n:n+2], uint16(len(k)))
		n += 2
		n += copy(dst[n:], k)

		binary.BigEndian.PutUint32(dst[n:n+4], uint32(len(v)))
		n += 4

		for _, vv := range v {
			binary.BigEndian.PutUint64(dst[n:n+8], uint64(vv.UnixNano()))
			n += 8

			switch vv := vv.(type) {
			case types.FloatValue:
				if curType != float64EntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[n:n+8], math.Float64bits(vv.RawValue()))
				n += 8
			case types.IntegerValue:
				if curType != integerEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[n:n+8], uint64(vv.RawValue()))
				n += 8
			case types.UnsignedValue:
				if curType != unsignedEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[n:n+8], vv.RawValue())
				n += 8
			case types.BooleanValue:
				if curType != booleanEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				if vv.RawValue() {
					dst[n] = 1
				} else {
					dst[n] = 0
				}
				n++
			case types.StringValue:
				if curType != stringEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint32(dst[n:n+4], uint32(len(vv.RawValue())))
				n += 4
				n += copy(dst[n:], vv.RawValue())
			default:
				return nil, fmt.Errorf("unsupported value found in %T slice: %T", v[0].Value(), vv)
			}
		}
	}

	return dst[:n], nil
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *WriteWALEntry) MarshalBinary() ([]byte, error) {
	// Temp buffer to write marshaled points into
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *WriteWALEntry) UnmarshalBinary(b []byte) error {
	var i int
	for i < len(b) {
		typ := b[i]
		i++

		if i+2 > len(b) {
			return errs.ErrWALCorrupt
		}

		length := int(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2

		if i+length > len(b) {
			return errs.ErrWALCorrupt
		}

		k := string(b[i : i+length])
		i += length

		if i+4 > len(b) {
			return errs.ErrWALCorrupt
		}

		nvals := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4

		if nvals <= 0 || nvals > len(b) {
			return errs.ErrWALCorrupt
		}

		switch typ {
		case float64EntryType:
			if i+16*nvals > len(b) {
				return errs.ErrWALCorrupt
			}

			values := make([]types.Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := math.Float64frombits(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				values = append(values, types.NewFloatValue(un, v))
			}
			w.Values[k] = values
		case integerEntryType:
			if i+16*nvals > len(b) {
				return errs.ErrWALCorrupt
			}

			values := make([]types.Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				values = append(values, types.NewIntegerValue(un, v))
			}
			w.Values[k] = values

		case unsignedEntryType:
			if i+16*nvals > len(b) {
				return errs.ErrWALCorrupt
			}

			values := make([]types.Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := binary.BigEndian.Uint64(b[i : i+8])
				i += 8
				values = append(values, types.NewUnsignedValue(un, v))
			}
			w.Values[k] = values

		case booleanEntryType:
			if i+9*nvals > len(b) {
				return errs.ErrWALCorrupt
			}

			values := make([]types.Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8

				v := b[i]
				i += 1
				if v == 1 {
					values = append(values, types.NewBooleanValue(un, true))
				} else {
					values = append(values, types.NewBooleanValue(un, false))
				}
			}
			w.Values[k] = values

		case stringEntryType:
			values := make([]types.Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				if i+12 > len(b) {
					return errs.ErrWALCorrupt
				}

				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8

				length := int(binary.BigEndian.Uint32(b[i : i+4]))
				if i+length > len(b) {
					return errs.ErrWALCorrupt
				}

				i += 4

				if i+length > len(b) {
					return errs.ErrWALCorrupt
				}

				v := string(b[i : i+length])
				i += length
				values = append(values, types.NewStringValue(un, v))
			}
			w.Values[k] = values

		default:
			return fmt.Errorf("unsupported value types: %#v", typ)
		}
	}
	return nil
}

// Type returns WriteWALEntryType.
func (w *WriteWALEntry) Type() WalEntryType {
	return WriteWALEntryType
}

// DeleteWALEntry represents the deletion of multiple series.
type DeleteWALEntry struct {
	Keys [][]byte
	sz   int
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *DeleteWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *DeleteWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	// b originates from a pool. Copy what needs to be retained.
	buf := make([]byte, len(b))
	copy(buf, b)
	w.Keys = bytes.Split(buf, []byte("\n"))
	return nil
}

func (w *DeleteWALEntry) MarshalSize() int {
	if w.sz > 0 || len(w.Keys) == 0 {
		return w.sz
	}

	encLen := len(w.Keys) // newlines
	for _, k := range w.Keys {
		encLen += len(k)
	}

	w.sz = encLen

	return encLen
}

// Encode converts the DeleteWALEntry into a byte slice, appending to dst.
func (w *DeleteWALEntry) Encode(dst []byte) ([]byte, error) {
	sz := w.MarshalSize()

	if len(dst) < sz {
		dst = make([]byte, sz)
	}

	var n int
	for _, k := range w.Keys {
		n += copy(dst[n:], k)
		n += copy(dst[n:], "\n")
	}

	// We return n-1 to strip off the last newline so that unmarshalling the value
	// does not produce an empty string
	return dst[:n-1], nil
}

// Type returns DeleteWALEntryType.
func (w *DeleteWALEntry) Type() WalEntryType {
	return DeleteWALEntryType
}

// DeleteRangeWALEntry represents the deletion of multiple series.
type DeleteRangeWALEntry struct {
	Keys     [][]byte
	Min, Max int64
	sz       int
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *DeleteRangeWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *DeleteRangeWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) < 16 {
		return errs.ErrWALCorrupt
	}

	w.Min = int64(binary.BigEndian.Uint64(b[:8]))
	w.Max = int64(binary.BigEndian.Uint64(b[8:16]))

	i := 16
	for i < len(b) {
		if i+4 > len(b) {
			return errs.ErrWALCorrupt
		}
		sz := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4

		if i+sz > len(b) {
			return errs.ErrWALCorrupt
		}

		// b originates from a pool. Copy what needs to be retained.
		buf := make([]byte, sz)
		copy(buf, b[i:i+sz])
		w.Keys = append(w.Keys, buf)
		i += sz
	}
	return nil
}

func (w *DeleteRangeWALEntry) MarshalSize() int {
	if w.sz > 0 {
		return w.sz
	}

	sz := 16 + len(w.Keys)*4
	for _, k := range w.Keys {
		sz += len(k)
	}

	w.sz = sz

	return sz
}

// Encode converts the DeleteRangeWALEntry into a byte slice, appending to b.
func (w *DeleteRangeWALEntry) Encode(b []byte) ([]byte, error) {
	sz := w.MarshalSize()

	if len(b) < sz {
		b = make([]byte, sz)
	}

	binary.BigEndian.PutUint64(b[:8], uint64(w.Min))
	binary.BigEndian.PutUint64(b[8:16], uint64(w.Max))

	i := 16
	for _, k := range w.Keys {
		binary.BigEndian.PutUint32(b[i:i+4], uint32(len(k)))
		i += 4
		i += copy(b[i:], k)
	}

	return b[:i], nil
}

// Type returns DeleteRangeWALEntryType.
func (w *DeleteRangeWALEntry) Type() WalEntryType {
	return DeleteRangeWALEntryType
}

type WalFile struct {
	fid  uint32
	size uint32 // so the max size of a wal file will not exceed 4 GB
	Pos  uint32

	path string
	lock sync.RWMutex

	*internel.MMapFile
}

func NewWalFile(fid int, dir string) *WalFile {
	return &WalFile{
		fid:  uint32(fid),
		path: walFilePath(dir, fid),
	}
}

func (f *WalFile) Open(flags int, size int) error {
	mmf, err := internel.OpenMmapFile(f.path, flags, size)

	if err != nil {
		return errs.Errorf(err, "while opening file: %s", f.path)
	}

	f.MMapFile = mmf
	f.size = uint32(len(f.Data))
	if mmf.NewFile {
		f.size = 0
	}

	return nil
}

func (f *WalFile) Truncate(end int64) error {
	if fs, err := f.Fd.Stat(); err != nil {
		return errs.Errorf(err, "while get stat from file: %s", f.path)
	} else if fs.Size() == end {
		return nil
	}

	f.size = uint32(end)
	return f.MMapFile.Truncate(end)
}

func (f *WalFile) Flush(offset uint32) error {
	if err := f.Sync(); err != nil {
		return errs.Errorf(err, "sync file: %s", f.path)
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.Truncate(int64(offset)); err != nil {
		return err
	}

	return nil
}

func (f *WalFile) WriteMulti(values map[string][]types.Value) error {
	entry := &WriteWALEntry{
		Values: values,
	}

	err := f.writeEntryToWal(entry)
	if err != nil {
		return err
	}

	return nil
}

// Remove the given keys
func (f *WalFile) Remove(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	entry := &DeleteWALEntry{
		Keys: keys,
	}

	err := f.writeEntryToWal(entry)
	if err != nil {
		return err
	}
	return nil
}

// RemoveRange deletes the given keys within the given time range,
// returning the segment ID for the operation.
func (f *WalFile) RemoveRange(keys [][]byte, min, max int64) error {
	if len(keys) == 0 {
		return nil
	}
	entry := &DeleteRangeWALEntry{
		Keys: keys,
		Min:  min,
		Max:  max,
	}

	err := f.writeEntryToWal(entry)
	if err != nil {
		return err
	}
	return nil
}

func (f *WalFile) writeEntryToWal(entry WALEntry) error {
	data := bytesPool.Get(entry.MarshalSize())
	defer bytesPool.Put(data)

	b, err := entry.Encode(data)
	if err != nil {
		return err
	}

	encBuf := bytesPool.Get(snappy.MaxEncodedLen(len(b)))
	defer bytesPool.Put(encBuf)

	compressedData := snappy.Encode(encBuf, b)

	header := make([]byte, 5)
	header[0] = byte(entry.Type())
	binary.BigEndian.PutUint32(header[1:5], uint32(len(compressedData)))

	err = f.write(header)
	if err != nil {
		return err
	}

	err = f.write(compressedData)
	if err != nil {
		return err
	}

	return nil
}

func (f *WalFile) write(data []byte) error {
	n := len(data)
	newPos := atomic.AddUint32(&f.Pos, uint32(n))

	if int(newPos) >= len(f.Data) {
		if err := f.Truncate(int64(newPos)); err != nil {
			return err
		}
	}

	start := int(newPos) - n
	errs.CheckArgument(n == copy(f.Data[start:], data))
	atomic.AddUint32(&f.size, uint32(n))

	return nil
}

func walFilePath(dir string, fid int) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, WALFileExtension))
}
