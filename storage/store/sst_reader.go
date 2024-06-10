package store

import (
	"equinox/storage/types"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// ErrFileInUse is returned when attempting to remove or close a TSM file that is still being used.
var ErrFileInUse = fmt.Errorf("file still in use")

// nilOffset is the value written to the offsets to indicate that position is deleted.  The value is the max
// uint32 which is an invalid position.  We don't use 0 as 0 is actually a valid position.
var nilOffset = []byte{255, 255, 255, 255}

// TSMReader is a reader for a TSM file.
type TSMReader struct {
	// refs is the count of active references to this reader.
	refs   int64
	refsWG sync.WaitGroup

	madviseWillNeed bool // Hint to the kernel with MADV_WILLNEED.
	mu              sync.RWMutex

	// accessor provides access and decoding of blocks for the reader.
	accessor blockAccessor

	// index is the index of all blocks.
	index TSMIndex

	// tombstoner ensures tombstoned keys are not available by the index.
	tombstoner *Tombstoner

	// size is the size of the file on disk.
	size int64

	// lastModified is the last time this file was modified on disk
	lastModified int64

	// deleteMu limits concurrent deletes
	deleteMu sync.Mutex
}

type tsmReaderOption func(*TSMReader)

// NewTSMReader returns a new TSMReader from the given file.
func NewTSMReader(f *os.File, options ...tsmReaderOption) (*TSMReader, error) {
	t := &TSMReader{}
	for _, option := range options {
		option(t)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	t.size = stat.Size()
	t.lastModified = stat.ModTime().UnixNano()
	t.accessor = &mmapAccessor{
		f:            f,
		mmapWillNeed: t.madviseWillNeed,
	}

	index, err := t.accessor.init()
	if err != nil {
		_ = t.accessor.close()
		return nil, err
	}

	t.index = index
	t.tombstoner = NewTombstoner(t.Path(), index.ContainsKey)

	if err := t.applyTombstones(); err != nil {
		return nil, err
	}

	return t, nil
}

func (r *TSMReader) applyTombstones() error {
	var cur, prev Tombstone
	batch := make([][]byte, 0, 4096)

	if err := r.tombstoner.Walk(func(ts Tombstone) error {
		cur = ts
		if len(batch) > 0 {
			if prev.Min != cur.Min || prev.Max != cur.Max {
				r.index.DeleteRange(batch, prev.Min, prev.Max)
				batch = batch[:0]
			}
		}

		// Copy the tombstone key and re-use the buffers to avoid allocations
		n := len(batch)
		batch = batch[:n+1]
		if cap(batch[n]) < len(ts.Key) {
			batch[n] = make([]byte, len(ts.Key))
		} else {
			batch[n] = batch[n][:len(ts.Key)]
		}
		copy(batch[n], ts.Key)

		if len(batch) >= 4096 {
			r.index.DeleteRange(batch, prev.Min, prev.Max)
			batch = batch[:0]
		}

		prev = ts
		return nil
	}); err != nil {
		return fmt.Errorf("init: read tombstones: %v", err)
	}

	if len(batch) > 0 {
		r.index.DeleteRange(batch, cur.Min, cur.Max)
	}
	return nil
}

func (r *TSMReader) Free() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.accessor.free()
}

// Path returns the path of the file the TSMReader was initialized with.
func (r *TSMReader) Path() string {
	r.mu.RLock()
	p := r.accessor.path()
	r.mu.RUnlock()
	return p
}

// Key returns the key and the underlying entry at the numeric index.
func (r *TSMReader) Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	return r.index.Key(index, entries)
}

// KeyAt returns the key and key type at position idx in the index.
func (r *TSMReader) KeyAt(idx int) ([]byte, byte) {
	return r.index.KeyAt(idx)
}

func (r *TSMReader) Seek(key []byte) int {
	return r.index.Seek(key)
}

// ReadAt returns the values corresponding to the given index entry.
func (r *TSMReader) ReadAt(entry *IndexEntry, vals []types.Value) ([]types.Value, error) {
	r.mu.RLock()
	v, err := r.accessor.readBlock(entry, vals)
	r.mu.RUnlock()
	return v, err
}

// Read returns the values corresponding to the block at the given key and timestamp.
func (r *TSMReader) Read(key []byte, timestamp int64) ([]types.Value, error) {
	r.mu.RLock()
	v, err := r.accessor.read(key, timestamp)
	r.mu.RUnlock()
	return v, err
}

// ReadAll returns all values for a key in all blocks.
func (r *TSMReader) ReadAll(key []byte) ([]types.Value, error) {
	r.mu.RLock()
	v, err := r.accessor.readAll(key)
	r.mu.RUnlock()
	return v, err
}

func (r *TSMReader) ReadBytes(e *IndexEntry, b []byte) (uint32, []byte, error) {
	r.mu.RLock()
	n, v, err := r.accessor.readBytes(e, b)
	r.mu.RUnlock()
	return n, v, err
}

// Type returns the type of values stored at the given key.
func (r *TSMReader) Type(key []byte) (byte, error) {
	return r.index.Type(key)
}

// Close closes the TSMReader.
func (r *TSMReader) Close() error {
	r.refsWG.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.accessor.close(); err != nil {
		return err
	}

	return r.index.Close()
}

// Ref records a usage of this TSMReader.  If there are active references
// when the reader is closed or removed, the reader will remain open until
// there are no more references.
func (r *TSMReader) Ref() {
	atomic.AddInt64(&r.refs, 1)
	r.refsWG.Add(1)
}

// Unref removes a usage record of this TSMReader.  If the Reader was closed
// by another goroutine while there were active references, the file will
// be closed and remove
func (r *TSMReader) Unref() {
	atomic.AddInt64(&r.refs, -1)
	r.refsWG.Done()
}

// InUse returns whether the TSMReader currently has any active references.
func (r *TSMReader) InUse() bool {
	refs := atomic.LoadInt64(&r.refs)
	return refs > 0
}

// Remove removes any underlying files stored on disk for this reader.
func (r *TSMReader) Remove() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.remove()
}

// Rename renames the underlying file to the new path.
func (r *TSMReader) Rename(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accessor.rename(path)
}

// Remove removes any underlying files stored on disk for this reader.
func (r *TSMReader) remove() error {
	path := r.accessor.path()

	if r.InUse() {
		return ErrFileInUse
	}

	if path != "" {
		err := os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	if err := r.tombstoner.Delete(); err != nil {
		return err
	}
	return nil
}

// Contains returns whether the given key is present in the index.
func (r *TSMReader) Contains(key []byte) bool {
	return r.index.Contains(key)
}

// ContainsValue returns true if key and time might exists in this file.  This function could
// return true even though the actual point does not exist.  For example, the key may
// exist in this file, but not have a point exactly at time t.
func (r *TSMReader) ContainsValue(key []byte, ts int64) bool {
	return r.index.ContainsValue(key, ts)
}

// DeleteRange removes the given points for keys between minTime and maxTime.   The series
// keys passed in must be sorted.
func (r *TSMReader) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	batch := r.BatchDelete()
	if err := batch.DeleteRange(keys, minTime, maxTime); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Delete deletes blocks indicated by keys.
func (r *TSMReader) Delete(keys [][]byte) error {
	if err := r.tombstoner.Add(keys); err != nil {
		return err
	}

	if err := r.tombstoner.Flush(); err != nil {
		return err
	}

	r.index.Delete(keys)
	return nil
}

func (r *TSMReader) BatchDelete() BatchDeleter {
	r.deleteMu.Lock()
	return &batchDelete{r: r}
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (r *TSMReader) OverlapsTimeRange(min, max int64) bool {
	return r.index.OverlapsTimeRange(min, max)
}

// OverlapsKeyRange returns true if the key range of the file intersect min and max.
func (r *TSMReader) OverlapsKeyRange(min, max []byte) bool {
	return r.index.OverlapsKeyRange(min, max)
}

// TimeRange returns the min and max time across all keys in the file.
func (r *TSMReader) TimeRange() (int64, int64) {
	return r.index.TimeRange()
}

// KeyRange returns the min and max key across all keys in the file.
func (r *TSMReader) KeyRange() ([]byte, []byte) {
	return r.index.KeyRange()
}

// KeyCount returns the count of unique keys in the TSMReader.
func (r *TSMReader) KeyCount() int {
	return r.index.KeyCount()
}

// Entries returns all index entries for key.
func (r *TSMReader) Entries(key []byte) []IndexEntry {
	return r.index.Entries(key)
}

// ReadEntries reads the index entries for key into entries.
func (r *TSMReader) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
	return r.index.ReadEntries(key, entries)
}

// IndexSize returns the size of the index in bytes.
func (r *TSMReader) IndexSize() uint32 {
	return r.index.Size()
}

// Size returns the size of the underlying file in bytes.
func (r *TSMReader) Size() uint32 {
	r.mu.RLock()
	size := r.size
	r.mu.RUnlock()
	return uint32(size)
}

// HasTombstones return true if there are any tombstone entries recorded.
func (r *TSMReader) HasTombstones() bool {
	r.mu.RLock()
	b := r.tombstoner.HasTombstones()
	r.mu.RUnlock()
	return b
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (r *TSMReader) TombstoneRange(key []byte) []TimeRange {
	r.mu.RLock()
	tr := r.index.TombstoneRange(key)
	r.mu.RUnlock()
	return tr
}

// BlockIterator returns a BlockIterator for the underlying TSM file.
func (r *TSMReader) BlockIterator() *BlockIterator {
	return &BlockIterator{
		r: r,
		n: r.index.KeyCount(),
	}
}

// ReadFloatBlockAt returns the float values corresponding to the given index entry.
func (r *TSMReader) ReadFloatBlockAt(entry *IndexEntry, vals *[]types.FloatValue) ([]types.FloatValue, error) {
	r.mu.RLock()
	v, err := r.accessor.readFloatBlock(entry, vals)
	r.mu.RUnlock()
	return v, err
}

// ReadFloatArrayBlockAt fills vals with the float values corresponding to the given index entry.
func (r *TSMReader) ReadFloatArrayBlockAt(entry *IndexEntry, vals *types.FloatArray) error {
	r.mu.RLock()
	err := r.accessor.readFloatArrayBlock(entry, vals)
	r.mu.RUnlock()
	return err
}

// ReadIntegerBlockAt returns the integer values corresponding to the given index entry.
func (r *TSMReader) ReadIntegerBlockAt(entry *IndexEntry, vals *[]types.IntegerValue) ([]types.IntegerValue, error) {
	r.mu.RLock()
	v, err := r.accessor.readIntegerBlock(entry, vals)
	r.mu.RUnlock()
	return v, err
}

// ReadIntegerArrayBlockAt fills vals with the integer values corresponding to the given index entry.
func (r *TSMReader) ReadIntegerArrayBlockAt(entry *IndexEntry, vals *types.IntegerArray) error {
	r.mu.RLock()
	err := r.accessor.readIntegerArrayBlock(entry, vals)
	r.mu.RUnlock()
	return err
}

// ReadUnsignedBlockAt returns the unsigned values corresponding to the given index entry.
func (r *TSMReader) ReadUnsignedBlockAt(entry *IndexEntry, vals *[]types.UnsignedValue) ([]types.UnsignedValue, error) {
	r.mu.RLock()
	v, err := r.accessor.readUnsignedBlock(entry, vals)
	r.mu.RUnlock()
	return v, err
}

// ReadUnsignedArrayBlockAt fills vals with the unsigned values corresponding to the given index entry.
func (r *TSMReader) ReadUnsignedArrayBlockAt(entry *IndexEntry, vals *types.UnsignedArray) error {
	r.mu.RLock()
	err := r.accessor.readUnsignedArrayBlock(entry, vals)
	r.mu.RUnlock()
	return err
}

// ReadStringBlockAt returns the string values corresponding to the given index entry.
func (r *TSMReader) ReadStringBlockAt(entry *IndexEntry, vals *[]types.StringValue) ([]types.StringValue, error) {
	r.mu.RLock()
	v, err := r.accessor.readStringBlock(entry, vals)
	r.mu.RUnlock()
	return v, err
}

// ReadStringArrayBlockAt fills vals with the string values corresponding to the given index entry.
func (r *TSMReader) ReadStringArrayBlockAt(entry *IndexEntry, vals *types.StringArray) error {
	r.mu.RLock()
	err := r.accessor.readStringArrayBlock(entry, vals)
	r.mu.RUnlock()
	return err
}

// ReadBooleanBlockAt returns the boolean values corresponding to the given index entry.
func (r *TSMReader) ReadBooleanBlockAt(entry *IndexEntry, vals *[]types.BooleanValue) ([]types.BooleanValue, error) {
	r.mu.RLock()
	v, err := r.accessor.readBooleanBlock(entry, vals)
	r.mu.RUnlock()
	return v, err
}

// ReadBooleanArrayBlockAt fills vals with the boolean values corresponding to the given index entry.
func (r *TSMReader) ReadBooleanArrayBlockAt(entry *IndexEntry, vals *types.BooleanArray) error {
	r.mu.RLock()
	err := r.accessor.readBooleanArrayBlock(entry, vals)
	r.mu.RUnlock()
	return err
}
