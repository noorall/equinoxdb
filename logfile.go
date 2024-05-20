package equinox

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"equinox/internel"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	Value byte = 1 << iota
	Delete
	ValPtr
)

// because we use the varint encode, so the size of a header is variable,
// but the maximum size will be 11
const maxHeaderSize = 11

// a header for entry
type header struct {
	klen uint32
	vlen uint32

	recordType byte
}

// Encode encodes the header into []byte. The provided []byte should be atleast 5 bytes. The
// function will panic if out []byte isn't large enough to hold all the values.
// The encoded header looks like
// +------------+------------+--------------+
// | recordType | Key Length | Value Length |
// +------------+------------+--------------+
// Returns the actual size for the encoded header
func (h header) Encode(buf []byte) int {
	buf[0] = h.recordType
	i := 1
	i += binary.PutUvarint(buf[i:], uint64(h.klen))
	i += binary.PutUvarint(buf[i:], uint64(h.vlen))

	return i
}

// Decode decodes the given header from the byte slice.
// Returns the number of bytes read.
func (h *header) Decode(buf []byte) int {
	h.recordType = buf[0]
	i := 1
	// decode varint klen
	klen, kcount := binary.Uvarint(buf[i:])
	i += kcount
	h.klen = uint32(klen)
	// decode varint vlen
	vlen, vcount := binary.Uvarint(buf[i:])
	i += vcount
	h.vlen = uint32(vlen)

	return i
}

func (h *header) decodeFrom(reader *internel.HashReader) (int, error) {
	var err error

	h.recordType, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}

	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.klen = uint32(klen)

	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.vlen = uint32(vlen)

	return reader.BytesRead(), nil
}

// layout of entry
// +--------+-----+-------+-------+
// | header | key | value | crc32 |
// +--------+-----+-------+-------+
type entry struct {
	key   []byte
	value []byte

	offset uint32
	hlen   uint16
	rtype  byte
}

func (e *entry) estimateSize(threshold uint32) uint32 {
	klen, vlen := uint32(len(e.key)), uint32(len(e.value))

	return klen + vlen + 1
}

func (e *entry) checkWithThreshold(threshold uint32) bool {
	return len(e.value) < int(threshold)
}

func (e *entry) isEmpty() bool {
	return len(e.key) == 0
}

type entryReader struct {
	offset uint32
}

func (r *entryReader) read(reader io.Reader) (*entry, error) {
	hash := internel.NewHashReader(reader)
	var h header
	hlen, err := h.decodeFrom(hash)
	if err != nil {
		return nil, err
	}

	e, esz := &entry{}, hlen
	e.offset = r.offset
	e.hlen = uint16(hlen)
	buf := make([]byte, h.klen+h.vlen)
	if n, err := io.ReadFull(hash, buf[:]); err != nil {
		if err == io.EOF {
			err = ErrTruncating
		}

		return nil, err
	} else {
		esz += n
	}

	e.key = buf[:h.klen]
	e.value = buf[h.klen:]
	var crcBuf [crc32.Size]byte
	if n, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = ErrTruncating
		}
		return nil, err
	} else {
		esz += n
	}

	crc := binary.BigEndian.Uint32(crcBuf[:])
	if crc != hash.Sum32() {
		return nil, ErrTruncating
	}
	e.rtype = h.recordType

	r.offset += uint32(esz)

	return e, nil
}

// When the size of a value exceed the writeoptions.maxValueThreshold, a valPtr will be generated.
// Then the value will store in the lsm file, but valPtr will store in a value log.
type valPtr struct {
	fid    uint32
	len    uint32
	offset uint32
}

const vptrSize = unsafe.Sizeof(valPtr{})

// Encode encodes value Pointer into byte buffer.
func (p valPtr) Encode() []byte {
	buf := make([]byte, vptrSize)
	*(*valPtr)(unsafe.Pointer(&buf[0])) = p

	return buf
}

// Decode decodes the value pointer into the byte buffer.
func (p *valPtr) Decode(buf []byte) {
	copy(((*[vptrSize]byte)(unsafe.Pointer(p))[:]), buf[:vptrSize])
}

type walker func(e *entry, vp valPtr) error

type logFile struct {
	fid  uint32
	size uint32 // so the max size of a log file will not exceed 4 GB
	pos  uint32

	path string
	lock sync.RWMutex

	*internel.MmapFile
}

func (f *logFile) Open(path string, flags int, size int) error {
	mmf, err := internel.OpenMmapFile(path, flags, size)

	if err != nil {
		return Wrapf(err, "while opening file: %s", path)
	}

	f.MmapFile = mmf
	f.size = uint32(len(f.Data))
	if mmf.NewFile {
		f.size = 0
		f.clearEntryHeader()
	}

	return nil
}

func (f *logFile) Truncate(end int64) error {
	if fs, err := f.Fd.Stat(); err != nil {
		return Wrapf(err, "while get stat from file: %s", f.path)
	} else if fs.Size() == end {
		return nil
	}

	f.size = uint32(end)
	return f.MmapFile.Truncate(end)
}

func (f *logFile) EncodeEntryTo(e *entry, buf *bytes.Buffer) (uint32, error) {
	h := header{
		klen:       uint32(len(e.key)),
		vlen:       uint32(len(e.value)),
		recordType: byte(e.rtype),
	}

	hash := crc32.New(internel.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header
	var headerBuf [maxHeaderSize]byte
	sz := h.Encode(headerBuf[:])
	Check2(writer.Write(headerBuf[:sz]))

	// write key and value
	Check2(writer.Write(e.key))
	Check2(writer.Write(e.value))

	// write crc32
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	Check2(writer.Write(crcBuf[:]))

	return uint32(sz + len(e.key) + len(e.value) + crc32.Size), nil
}

func (f *logFile) WriteEntry(e *entry, buf *bytes.Buffer) error {
	buf.Reset()
	if _, err := f.EncodeEntryTo(e, buf); err != nil {
		return err
	}

	return f.WriteEntryFrom(buf)
}

func (f *logFile) WriteEntryFrom(buf *bytes.Buffer) error {
	n := buf.Len()

	newPos := atomic.AddUint32(&f.pos, uint32(n))
	if int(newPos) >= len(f.Data) {
		if err := f.Truncate(int64(newPos)); err != nil {
			return err
		}
	}

	start := int(newPos) - n

	AssertTrue(int(n) == copy(f.Data[start:], buf.Bytes()))
	atomic.AddUint32(&f.size, uint32(n))
	buf.Reset()

	return nil
}

func (f *logFile) ReadEntry(buf []byte, offset uint32) (*entry, error) {
	var h header
	hSize := h.Decode(buf)
	kv := buf[hSize:]

	e := &entry{
		key:    kv[:h.klen],
		value:  kv[h.klen : h.klen+h.vlen],
		rtype:  h.recordType,
		offset: offset,
		hlen:   uint16(hSize),
	}

	return e, nil
}

func (f *logFile) iterate(offset uint32, w walker) (uint32, error) {
	reader := bufio.NewReader(f.NewReader(int(offset)))
	entryReader := &entryReader{offset: offset}

	for {
		e, err := entryReader.read(reader)
		if err == io.EOF || err == io.ErrUnexpectedEOF || err == ErrTruncating {
			break
		} else if err != nil {
			return 0, nil
		}

		if e == nil {
			continue
		}

		if e.isEmpty() {
			break
		}

		vp := valPtr{
			fid:    f.fid,
			offset: e.offset,
			len:    entryReader.offset - e.offset,
		}

		if err := w(e, vp); err != nil {
			if err == ErrStopIteration {
				break
			}

			return 0, Wrapf(err, "walker function walk in file: %s", f.path)
		}
	}

	return entryReader.offset, nil
}

func (f *logFile) readWithValPtr(p valPtr) (buf []byte, err error) {
	offset := p.offset

	size := int64(len(f.Data))
	valSize := p.len
	fSize := int64(atomic.LoadUint32(&f.size))
	if int64(offset) >= size || int64(offset+valSize) > size ||
		int64(offset+valSize) > fSize {
		err = io.EOF
	} else {
		buf = f.Data[offset : offset+valSize]
	}

	return buf, err
}

func (f *logFile) Flush(offset uint32) error {
	if err := f.Sync(); err != nil {
		return Wrapf(err, "sync file: %s", f.path)
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.Truncate(int64(offset)); err != nil {
		return err
	}

	return nil
}

func (f *logFile) clearEntryHeader() {
	start, end := f.pos, f.pos+maxHeaderSize
	if end > f.size {
		return
	}

	if end > f.size {
		end = f.size
	}

	if end <= start {
		return
	}

	h := f.Data[start:end]

	for i := range h {
		h[i] = 0x0
	}
}
