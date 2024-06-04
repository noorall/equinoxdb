package store

import (
	"bufio"
	"encoding/binary"
	"equinox/storage/types"
	"fmt"
	"github.com/golang/snappy"
	"io"
)

type WALReader struct {
	r     *bufio.Reader
	entry WALEntry
	n     int64
	err   error
}

// NewWALReader returns a new WALReader reading from r.
func NewWALReader(r io.Reader) *WALReader {
	return &WALReader{
		r: bufio.NewReader(r),
	}
}

func (r *WALReader) Reset(reader io.Reader) {
	r.r.Reset(reader)
	r.entry = nil
	r.n = 0
	r.err = nil
}

// Next indicates if there is a value to read.
func (r *WALReader) Next() bool {
	var nReadOK int

	// read the type and the length of the entry
	var lv [5]byte
	n, err := io.ReadFull(r.r, lv[:])
	if err == io.EOF {
		return false
	}

	if err != nil {
		r.err = err
		// We return true here because we want the client code to call read which
		// will return the error to be handled.
		return true
	}
	nReadOK += n

	entryType := lv[0]
	length := binary.BigEndian.Uint32(lv[1:5])

	b := bytesSyncPool.Get(int(length))
	defer bytesSyncPool.Put(b)

	// read the compressed block and decompress it
	n, err = io.ReadFull(r.r, b[:length])
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n

	decLen, err := snappy.DecodedLen(b[:length])
	if err != nil {
		r.err = err
		return true
	}
	decBuf := bytesSyncPool.Get(decLen)
	defer bytesSyncPool.Put(decBuf)

	data, err := snappy.Decode(decBuf, b[:length])
	if err != nil {
		r.err = err
		return true
	}

	// and marshal it and send it to the cache
	switch WalEntryType(entryType) {
	case WriteWALEntryType:
		r.entry = &WriteWALEntry{
			Values: make(map[string][]types.Value),
		}
	case DeleteWALEntryType:
		r.entry = &DeleteWALEntry{}
	case DeleteRangeWALEntryType:
		r.entry = &DeleteRangeWALEntry{}
	default:
		r.err = fmt.Errorf("unknown wal entry type: %v", entryType)
		return true
	}
	r.err = r.entry.UnmarshalBinary(data)
	if r.err == nil {
		// Read and decode of this entry was successful.
		r.n += int64(nReadOK)
	}

	return true
}

// Read returns the next entry in the reader.
func (r *WALReader) Read() (WALEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.entry, nil
}

// Count returns the total number of bytes read successfully from the segment, as
// of the last call to Read(). The segment is guaranteed to be valid up to and
// including this number of bytes.
func (r *WALReader) Count() int64 {
	return r.n
}

// Error returns the last error encountered by the reader.
func (r *WALReader) Error() error {
	return r.err
}
