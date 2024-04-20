package internel

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"reflect"
	"sync"
	"unsafe"
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

type HashReader struct {
	r io.Reader
	h hash.Hash32

	bytesRead int
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)

	return &HashReader{
		r: r,
		h: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil {
		return n, err
	}

	t.bytesRead += n
	return t.h.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)

	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *HashReader) Sum32() uint32 {
	return t.h.Sum32()
}

func (t *HashReader) BytesRead() int {
	return t.bytesRead
}

func Min(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func Max(a, b int) int {
	if a >= b {
		return a
	}

	return b
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// U32SliceToBytes converts the given Uint32 slice to byte slice
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// BytesToU64 converts the given byte slice to uint64
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U64SliceToBytes converts the given Uint64 slice to byte slice
func U64SliceToBytes(u64s []uint64) []byte {
	if len(u64s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u64s) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u64s[0]))
	return b
}

// BytesToU64Slice converts the given byte slice to uint64 slice
func BytesToU64Slice(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	var u64s []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u64s
}

func VerifyCheckSum(data []byte, checksum []byte) bool {
	actual := crc32.Checksum(data, CastagnoliCrcTable)
	expected := BytesToU32(checksum)
	return actual == expected
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// Throttle allows a limited number of workers to run at a time. It also
// provides a mechanism to check for errors encountered by workers and wait for
// them to finish.
type Throttle struct {
	one sync.Once
	wg  sync.WaitGroup

	numWorkers chan struct{}
	errs       chan error
	finishErr  error
}

// NewThrottle creates a new throttle with a max number of workers.
func NewThrottle(max int) *Throttle {
	return &Throttle{
		numWorkers: make(chan struct{}, max),
		errs:       make(chan error, max),
	}
}

// Do should be called by workers before they start working. It blocks if there
// are already maximum number of workers working. If it detects an error from
// previously Done workers, it would return it.
func (t *Throttle) Do() error {
	for {
		select {
		case t.numWorkers <- struct{}{}:
			t.wg.Add(1)
			return nil
		case err := <-t.errs:
			if err != nil {
				return err
			}
		}
	}
}

// Done should be called by workers when they finish working. They can also
// pass the error status of work done.
func (t *Throttle) Done(err error) {
	if err != nil {
		t.errs <- err
	}

	select {
	case <-t.numWorkers:
	default:
		panic("Throttle Do Done mismatch")
	}

	t.wg.Done()
}

// Finish waits until all workers have finished working. It would return any error passed by Done.
// If Finish is called multiple time, it will wait for workers to finish only once(first time).
// From next calls, it will return same error as found on first call.
func (t *Throttle) Finish() error {
	t.one.Do(func() {
		t.wg.Wait()
		close(t.numWorkers)
		close(t.errs)

		for err := range t.errs {
			if err != nil {
				t.finishErr = err
				return
			}
		}
	})

	return t.finishErr
}
