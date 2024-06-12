package store

import (
	"encoding/binary"
	"equinox/pkg/file"
	"equinox/storage/codec"
	"equinox/storage/types"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

var (
	// ErrTSMClosed is returned when performing an operation against a closed TSM file.
	ErrTSMClosed = fmt.Errorf("tsm file closed")
)

type blockAccessor interface {
	init() (*indirectIndex, error)
	read(key []byte, timestamp int64) ([]types.Value, error)
	readAll(key []byte) ([]types.Value, error)
	readBlock(entry *IndexEntry, values []types.Value) ([]types.Value, error)
	readFloatBlock(entry *IndexEntry, values *[]types.FloatValue) ([]types.FloatValue, error)
	readFloatArrayBlock(entry *IndexEntry, values *types.FloatArray) error
	readIntegerBlock(entry *IndexEntry, values *[]types.IntegerValue) ([]types.IntegerValue, error)
	readIntegerArrayBlock(entry *IndexEntry, values *types.IntegerArray) error
	readUnsignedBlock(entry *IndexEntry, values *[]types.UnsignedValue) ([]types.UnsignedValue, error)
	readUnsignedArrayBlock(entry *IndexEntry, values *types.UnsignedArray) error
	readStringBlock(entry *IndexEntry, values *[]types.StringValue) ([]types.StringValue, error)
	readStringArrayBlock(entry *IndexEntry, values *types.StringArray) error
	readBooleanBlock(entry *IndexEntry, values *[]types.BooleanValue) ([]types.BooleanValue, error)
	readBooleanArrayBlock(entry *IndexEntry, values *types.BooleanArray) error
	readBytes(entry *IndexEntry, buf []byte) (uint32, []byte, error)
	rename(path string) error
	path() string
	close() error
	free() error
}

type mmapAccessor struct {
	accessCount uint64 // Counter incremented everytime the mmapAccessor is accessed
	freeCount   uint64 // Counter to determine whether the accessor can free its resources

	mmapWillNeed bool // If true then mmap advise value MADV_WILLNEED will be provided the kernel for b.

	mu sync.RWMutex
	b  []byte
	f  *os.File

	index *indirectIndex
}

func (m *mmapAccessor) init() (*indirectIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := verifyVersion(m.f); err != nil {
		return nil, err
	}

	var err error

	if _, err := m.f.Seek(0, 0); err != nil {
		return nil, err
	}

	stat, err := m.f.Stat()
	if err != nil {
		return nil, err
	}

	m.b, err = mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return nil, err
	}
	if len(m.b) < 8 {
		return nil, fmt.Errorf("mmapAccessor: byte slice too small for indirectIndex")
	}

	// Hint to the kernel that we will be reading the file.  It would be better to hint
	// that we will be reading the index section, but that's not been
	// implemented as yet.
	if m.mmapWillNeed {
		if err := madviseWillNeed(m.b); err != nil {
			return nil, err
		}
	}

	indexOfsPos := len(m.b) - 8
	indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos : indexOfsPos+8])
	if indexStart >= uint64(indexOfsPos) {
		return nil, fmt.Errorf("mmapAccessor: invalid indexStart")
	}

	m.index = NewIndirectIndex()
	if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
		return nil, err
	}

	// Allow resources to be freed immediately if requested
	m.incAccess()
	atomic.StoreUint64(&m.freeCount, 1)

	return m.index, nil
}

func (m *mmapAccessor) free() error {
	accessCount := atomic.LoadUint64(&m.accessCount)
	freeCount := atomic.LoadUint64(&m.freeCount)

	// Already freed everything.
	if freeCount == 0 && accessCount == 0 {
		return nil
	}

	// Were there accesses after the last time we tried to free?
	// If so, don't free anything and record the access count that we
	// see now for the next check.
	if accessCount != freeCount {
		atomic.StoreUint64(&m.freeCount, accessCount)
		return nil
	}

	// Reset both counters to zero to indicate that we have freed everything.
	atomic.StoreUint64(&m.accessCount, 0)
	atomic.StoreUint64(&m.freeCount, 0)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return madviseDontNeed(m.b)
}

func (m *mmapAccessor) incAccess() {
	atomic.AddUint64(&m.accessCount, 1)
}

func (m *mmapAccessor) rename(path string) error {
	m.incAccess()

	m.mu.Lock()
	defer m.mu.Unlock()

	err := munmap(m.b)
	if err != nil {
		return err
	}

	if err := m.f.Close(); err != nil {
		return err
	}

	if err := file.RenameFile(m.f.Name(), path); err != nil {
		return err
	}

	m.f, err = os.Open(path)
	if err != nil {
		return err
	}

	if _, err := m.f.Seek(0, 0); err != nil {
		return err
	}

	stat, err := m.f.Stat()
	if err != nil {
		return err
	}

	m.b, err = mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return err
	}

	if m.mmapWillNeed {
		return madviseWillNeed(m.b)
	}
	return nil
}

func (m *mmapAccessor) read(key []byte, timestamp int64) ([]types.Value, error) {
	entry := m.index.Entry(key, timestamp)
	if entry == nil {
		return nil, nil
	}

	return m.readBlock(entry, nil)
}

func (m *mmapAccessor) readBlock(entry *IndexEntry, values []types.Value) ([]types.Value, error) {
	m.incAccess()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = codec.DecodeBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readBytes(entry *IndexEntry, b []byte) (uint32, []byte, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return 0, nil, ErrTSMClosed
	}

	// return the bytes after the 4 byte checksum
	crc, block := binary.BigEndian.Uint32(m.b[entry.Offset:entry.Offset+4]), m.b[entry.Offset+4:entry.Offset+int64(entry.Size)]
	m.mu.RUnlock()

	return crc, block, nil
}

// readAll returns all values for a key in all blocks.
func (m *mmapAccessor) readAll(key []byte) ([]types.Value, error) {
	m.incAccess()

	blocks := m.index.Entries(key)
	if len(blocks) == 0 {
		return nil, nil
	}

	tombstones := m.index.TombstoneRange(key)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var temp []types.Value
	var err error
	var values []types.Value
	for _, block := range blocks {
		var skip bool
		for _, t := range tombstones {
			// Should we skip this block because it contains points that have been deleted
			if t.Min <= block.MinTime && t.Max >= block.MaxTime {
				skip = true
				break
			}
		}

		if skip {
			continue
		}
		//TODO: Validate checksum
		temp = temp[:0]
		// The +4 is the 4 byte checksum length
		temp, err = codec.DecodeBlock(m.b[block.Offset+4:block.Offset+int64(block.Size)], temp)
		if err != nil {
			return nil, err
		}

		// Filter out any values that were deleted
		for _, t := range tombstones {
			temp = types.Values(temp).Exclude(t.Min, t.Max)
		}

		values = append(values, temp...)
	}

	return values, nil
}

func (m *mmapAccessor) path() string {
	m.mu.RLock()
	path := m.f.Name()
	m.mu.RUnlock()
	return path
}

func (m *mmapAccessor) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.b == nil {
		return nil
	}

	err := munmap(m.b)
	if err != nil {
		return err
	}

	m.b = nil
	return m.f.Close()
}

func (m *mmapAccessor) readFloatBlock(entry *IndexEntry, values *[]types.FloatValue) ([]types.FloatValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := codec.DecodeFloatBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readFloatArrayBlock(entry *IndexEntry, values *types.FloatArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := codec.DecodeFloatArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readIntegerBlock(entry *IndexEntry, values *[]types.IntegerValue) ([]types.IntegerValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := codec.DecodeIntegerBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readIntegerArrayBlock(entry *IndexEntry, values *types.IntegerArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := codec.DecodeIntegerArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readUnsignedBlock(entry *IndexEntry, values *[]types.UnsignedValue) ([]types.UnsignedValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := codec.DecodeUnsignedBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readUnsignedArrayBlock(entry *IndexEntry, values *types.UnsignedArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := codec.DecodeUnsignedArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readStringBlock(entry *IndexEntry, values *[]types.StringValue) ([]types.StringValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := codec.DecodeStringBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readStringArrayBlock(entry *IndexEntry, values *types.StringArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := codec.DecodeStringArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readBooleanBlock(entry *IndexEntry, values *[]types.BooleanValue) ([]types.BooleanValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := codec.DecodeBooleanBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readBooleanArrayBlock(entry *IndexEntry, values *types.BooleanArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := codec.DecodeBooleanArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}
