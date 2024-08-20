package store

import (
	"equinox/storage/types"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// ErrFileInUse is returned when attempting to remove or close a SST file that is still being used.
var ErrFileInUse = fmt.Errorf("file still in use")

// nilOffset is the value written to the offsets to indicate that position is deleted.  The value is the max
// uint32 which is an invalid position.  We don't use 0 as 0 is actually a valid position.
var nilOffset = []byte{255, 255, 255, 255}

var WithMadviseWillNeed = func(willNeed bool) sstReaderOption {
	return func(r *SSTReader) {
		r.madviseWillNeed = willNeed
	}
}

// SSTReader is a reader for a SST file.
type SSTReader struct {
	// refs is the count of active references to this reader.
	refs   int64
	refsWG sync.WaitGroup

	madviseWillNeed bool // Hint to the kernel with MADV_WILLNEED.
	mu              sync.RWMutex

	// accessor provides access and decoding of blocks for the reader.
	accessor blockAccessor

	// index is the index of all blocks.
	index SSTIndex

	// tombstoner ensures tombstoned keys are not available by the index.
	tombstoner *Tombstoner

	// size is the size of the file on disk.
	size int64

	// lastModified is the last time this file was modified on disk
	lastModified int64

	// deleteMu limits concurrent deletes
	deleteMu sync.Mutex
}

type BlockIterator struct {
	R *SSTReader

	// i is the current key index
	i int

	// n is the total number of keys
	n int

	key     []byte
	cache   []IndexEntry
	entries []IndexEntry
	err     error
	typ     byte
}

// PeekNext returns the next key to be iterated or an empty string.
func (b *BlockIterator) PeekNext() []byte {
	if len(b.entries) > 1 {
		return b.key
	} else if b.n-b.i > 1 {
		key, _ := b.R.KeyAt(b.i + 1)
		return key
	}
	return nil
}

// Next returns true if there are more blocks to iterate through.
func (b *BlockIterator) Next() bool {
	if b.err != nil {
		return false
	}

	if b.n-b.i == 0 && len(b.entries) == 0 {
		return false
	}

	if len(b.entries) > 0 {
		b.entries = b.entries[1:]
		if len(b.entries) > 0 {
			return true
		}
	}

	if b.n-b.i > 0 {
		b.key, b.typ, b.entries = b.R.Key(b.i, &b.cache)
		b.i++

		// If there were deletes on the SSTReader, then our index is now off and we
		// can't proceed.  What we just read may not actually the next block.
		if b.n != b.R.KeyCount() {
			b.err = fmt.Errorf("delete during iteration")
			return false
		}

		if len(b.entries) > 0 {
			return true
		}
	}

	return false
}

// Read reads information about the next block to be iterated.
func (b *BlockIterator) Read() (key []byte, minTime int64, maxTime int64, typ byte, checksum uint32, buf []byte, err error) {
	if b.err != nil {
		return nil, 0, 0, 0, 0, nil, b.err
	}
	checksum, buf, err = b.R.ReadBytes(&b.entries[0], nil)
	if err != nil {
		b.err = err
		return nil, 0, 0, 0, 0, nil, err
	}
	return b.key, b.entries[0].MinTime, b.entries[0].MaxTime, b.typ, checksum, buf, err
}

// Err returns any errors encounter during iteration.
func (b *BlockIterator) Err() error {
	return b.err
}

type sstReaderOption func(*SSTReader)

// NewSSTReader returns a new SSTReader from the given file.
func NewSSTReader(f *os.File, vr *VFileRegionManager, options ...sstReaderOption) (*SSTReader, error) {
	t := &SSTReader{}
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
		vr:           vr,
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

func (t *SSTReader) applyTombstones() error {
	var cur, prev Tombstone
	batch := make([][]byte, 0, 4096)

	if err := t.tombstoner.Walk(func(ts Tombstone) error {
		cur = ts
		if len(batch) > 0 {
			if prev.Min != cur.Min || prev.Max != cur.Max {
				t.index.DeleteRange(batch, prev.Min, prev.Max)
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
			t.index.DeleteRange(batch, prev.Min, prev.Max)
			batch = batch[:0]
		}

		prev = ts
		return nil
	}); err != nil {
		return fmt.Errorf("init: read tombstones: %v", err)
	}

	if len(batch) > 0 {
		t.index.DeleteRange(batch, cur.Min, cur.Max)
	}
	return nil
}

func (t *SSTReader) Free() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.free()
}

// Path returns the path of the file the SSTReader was initialized with.
func (t *SSTReader) Path() string {
	t.mu.RLock()
	p := t.accessor.path()
	t.mu.RUnlock()
	return p
}

// Key returns the key and the underlying entry at the numeric index.
func (t *SSTReader) Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	return t.index.Key(index, entries)
}

// KeyAt returns the key and key type at position idx in the index.
func (t *SSTReader) KeyAt(idx int) ([]byte, byte) {
	return t.index.KeyAt(idx)
}

func (t *SSTReader) Seek(key []byte) int {
	return t.index.Seek(key)
}

// ReadAt returns the values corresponding to the given index entry.
func (t *SSTReader) ReadAt(entry *IndexEntry, vals []types.Value) ([]types.Value, error) {
	t.mu.RLock()
	v, err := t.accessor.readBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// Read returns the values corresponding to the block at the given key and timestamp.
func (t *SSTReader) Read(key []byte, timestamp int64) ([]types.Value, error) {
	t.mu.RLock()
	v, err := t.accessor.read(key, timestamp)
	t.mu.RUnlock()
	return v, err
}

// ReadAll returns all values for a key in all blocks.
func (t *SSTReader) ReadAll(key []byte) ([]types.Value, error) {
	t.mu.RLock()
	v, err := t.accessor.readAll(key)
	t.mu.RUnlock()
	return v, err
}

func (t *SSTReader) ReadBytes(e *IndexEntry, b []byte) (uint32, []byte, error) {
	t.mu.RLock()
	n, v, err := t.accessor.readBytes(e, b)
	t.mu.RUnlock()
	return n, v, err
}

// Type returns the type of values stored at the given key.
func (t *SSTReader) Type(key []byte) (byte, error) {
	return t.index.Type(key)
}

// Close closes the SSTReader.
func (t *SSTReader) Close() error {
	t.refsWG.Wait()

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.accessor.close(); err != nil {
		return err
	}

	return t.index.Close()
}

// Ref records a usage of this SSTReader.  If there are active references
// when the reader is closed or removed, the reader will remain open until
// there are no more references.
func (t *SSTReader) Ref() {
	atomic.AddInt64(&t.refs, 1)
	t.refsWG.Add(1)
}

// Unref removes a usage record of this SSTReader.  If the Reader was closed
// by another goroutine while there were active references, the file will
// be closed and remove
func (t *SSTReader) Unref() {
	atomic.AddInt64(&t.refs, -1)
	t.refsWG.Done()
}

// InUse returns whether the SSTReader currently has any active references.
func (t *SSTReader) InUse() bool {
	refs := atomic.LoadInt64(&t.refs)
	return refs > 0
}

// Remove removes any underlying files stored on disk for this reader.
func (t *SSTReader) Remove() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.remove()
}

// Rename renames the underlying file to the new path.
func (t *SSTReader) Rename(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.accessor.rename(path)
}

// Remove removes any underlying files stored on disk for this reader.
func (t *SSTReader) remove() error {
	path := t.accessor.path()

	if t.InUse() {
		return ErrFileInUse
	}

	if path != "" {
		err := os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	if err := t.tombstoner.Delete(); err != nil {
		return err
	}
	return nil
}

// Contains returns whether the given key is present in the index.
func (t *SSTReader) Contains(key []byte) bool {
	return t.index.Contains(key)
}

// ContainsValue returns true if key and time might exists in this file.  This function could
// return true even though the actual point does not exist.  For example, the key may
// exist in this file, but not have a point exactly at time t.
func (t *SSTReader) ContainsValue(key []byte, ts int64) bool {
	return t.index.ContainsValue(key, ts)
}

// DeleteRange removes the given points for keys between minTime and maxTime.   The series
// keys passed in must be sorted.
func (t *SSTReader) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	batch := t.BatchDelete()
	if err := batch.DeleteRange(keys, minTime, maxTime); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Delete deletes blocks indicated by keys.
func (t *SSTReader) Delete(keys [][]byte) error {
	if err := t.tombstoner.Add(keys); err != nil {
		return err
	}

	if err := t.tombstoner.Flush(); err != nil {
		return err
	}

	t.index.Delete(keys)
	return nil
}

func (t *SSTReader) BatchDelete() BatchDeleter {
	t.deleteMu.Lock()
	return &batchDelete{r: t}
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (t *SSTReader) OverlapsTimeRange(min, max int64) bool {
	return t.index.OverlapsTimeRange(min, max)
}

// OverlapsKeyRange returns true if the key range of the file intersect min and max.
func (t *SSTReader) OverlapsKeyRange(min, max []byte) bool {
	return t.index.OverlapsKeyRange(min, max)
}

// TimeRange returns the min and max time across all keys in the file.
func (t *SSTReader) TimeRange() (int64, int64) {
	return t.index.TimeRange()
}

// KeyRange returns the min and max key across all keys in the file.
func (t *SSTReader) KeyRange() ([]byte, []byte) {
	return t.index.KeyRange()
}

// KeyCount returns the count of unique keys in the SSTReader.
func (t *SSTReader) KeyCount() int {
	return t.index.KeyCount()
}

// Entries returns all index entries for key.
func (t *SSTReader) Entries(key []byte) []IndexEntry {
	return t.index.Entries(key)
}

// ReadEntries reads the index entries for key into entries.
func (t *SSTReader) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
	return t.index.ReadEntries(key, entries)
}

// IndexSize returns the size of the index in bytes.
func (t *SSTReader) IndexSize() uint32 {
	return t.index.Size()
}

// Size returns the size of the underlying file in bytes.
func (t *SSTReader) Size() uint32 {
	t.mu.RLock()
	size := t.size
	t.mu.RUnlock()
	return uint32(size)
}

// HasTombstones return true if there are any tombstone entries recorded.
func (t *SSTReader) HasTombstones() bool {
	t.mu.RLock()
	b := t.tombstoner.HasTombstones()
	t.mu.RUnlock()
	return b
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (t *SSTReader) TombstoneRange(key []byte) []TimeRange {
	t.mu.RLock()
	tr := t.index.TombstoneRange(key)
	t.mu.RUnlock()
	return tr
}

// BlockIterator returns a BlockIterator for the underlying SST file.
func (t *SSTReader) BlockIterator() *BlockIterator {
	return &BlockIterator{
		R: t,
		n: t.index.KeyCount(),
	}
}

// ReadFloatBlockAt returns the float values corresponding to the given index entry.
func (t *SSTReader) ReadFloatBlockAt(entry *IndexEntry, vals *[]types.FloatValue) ([]types.FloatValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readFloatBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadFloatArrayBlockAt fills vals with the float values corresponding to the given index entry.
func (t *SSTReader) ReadFloatArrayBlockAt(entry *IndexEntry, vals *types.FloatArray) error {
	t.mu.RLock()
	err := t.accessor.readFloatArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadIntegerBlockAt returns the integer values corresponding to the given index entry.
func (t *SSTReader) ReadIntegerBlockAt(entry *IndexEntry, vals *[]types.IntegerValue) ([]types.IntegerValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readIntegerBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadIntegerArrayBlockAt fills vals with the integer values corresponding to the given index entry.
func (t *SSTReader) ReadIntegerArrayBlockAt(entry *IndexEntry, vals *types.IntegerArray) error {
	t.mu.RLock()
	err := t.accessor.readIntegerArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadUnsignedBlockAt returns the unsigned values corresponding to the given index entry.
func (t *SSTReader) ReadUnsignedBlockAt(entry *IndexEntry, vals *[]types.UnsignedValue) ([]types.UnsignedValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readUnsignedBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadUnsignedArrayBlockAt fills vals with the unsigned values corresponding to the given index entry.
func (t *SSTReader) ReadUnsignedArrayBlockAt(entry *IndexEntry, vals *types.UnsignedArray) error {
	t.mu.RLock()
	err := t.accessor.readUnsignedArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadStringBlockAt returns the string values corresponding to the given index entry.
func (t *SSTReader) ReadStringBlockAt(entry *IndexEntry, vals *[]types.StringValue) ([]types.StringValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readStringBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadStringArrayBlockAt fills vals with the string values corresponding to the given index entry.
func (t *SSTReader) ReadStringArrayBlockAt(entry *IndexEntry, vals *types.StringArray) error {
	t.mu.RLock()
	err := t.accessor.readStringArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadBooleanBlockAt returns the boolean values corresponding to the given index entry.
func (t *SSTReader) ReadBooleanBlockAt(entry *IndexEntry, vals *[]types.BooleanValue) ([]types.BooleanValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readBooleanBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadBooleanArrayBlockAt fills vals with the boolean values corresponding to the given index entry.
func (t *SSTReader) ReadBooleanArrayBlockAt(entry *IndexEntry, vals *types.BooleanArray) error {
	t.mu.RLock()
	err := t.accessor.readBooleanArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

func (t *SSTReader) LastModified() int64 {
	t.mu.RLock()
	lm := t.lastModified
	if ts := t.tombstoner.TombstoneStats(); ts.TombstoneExists {
		if ts.LastModified > lm {
			lm = ts.LastModified
		}
	}
	t.mu.RUnlock()
	return lm
}

func (t *SSTReader) TombstoneStats() TombstoneStat {
	t.mu.RLock()
	fs := t.tombstoner.TombstoneStats()
	t.mu.RUnlock()
	return fs
}

func (t *SSTReader) Stats() FileStat {
	minTime, maxTime := t.index.TimeRange()
	minKey, maxKey := t.index.KeyRange()
	return FileStat{
		Path:         t.Path(),
		Size:         t.Size(),
		LastModified: t.LastModified(),
		MinTime:      minTime,
		MaxTime:      maxTime,
		MinKey:       minKey,
		MaxKey:       maxKey,
		HasTombstone: t.tombstoner.HasTombstones(),
	}
}
