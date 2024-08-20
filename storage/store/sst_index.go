package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
)

// SSTIndex represent the index section of a SST file.  The index records all
// blocks, their locations, sizes, min and max times.
type SSTIndex interface {
	// Delete removes the given keys from the index.
	Delete(keys [][]byte)

	// DeleteRange removes the given keys with data between minTime and maxTime from the index.
	DeleteRange(keys [][]byte, minTime, maxTime int64)

	// ContainsKey returns true if the given key may exist in the index.  This func is faster than
	// Contains but, may return false positives.
	ContainsKey(key []byte) bool

	// Contains return true if the given key exists in the index.
	Contains(key []byte) bool

	// ContainsValue returns true if key and time might exist in this file.  This function could
	// return true even though the actual point does not exists.  For example, the key may
	// exist in this file, but not have a point exactly at time t.
	ContainsValue(key []byte, timestamp int64) bool

	// Entries returns all index entries for a key.
	Entries(key []byte) []IndexEntry

	// ReadEntries reads the index entries for key into entries.
	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key and timestamp, nil is returned.
	Entry(key []byte, timestamp int64) *IndexEntry

	// Key returns the key in the index at the given position, using entries to avoid allocations.
	Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry)

	// KeyAt returns the key in the index at the given position.
	KeyAt(index int) ([]byte, byte)

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Seek returns the position in the index where key <= value in the index.
	Seek(key []byte) int

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
	OverlapsKeyRange(min, max []byte) bool

	// Size returns the size of the current index in bytes.
	Size() uint32

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// UnmarshalBinary populates an index from an encoded byte slice
	// representation of an index.
	UnmarshalBinary(b []byte) error

	// Close closes the index and releases any resources.
	Close() error
}

type indirectIndex struct {
	mu sync.RWMutex

	// indirectIndex works a follows.  Assuming we have an index structure in memory as
	// the diagram below:
	//
	// ┌────────────────────────────────────────────────────────────────────┐
	// │                               Index                                │
	// ├─┬──────────────────────┬──┬───────────────────────┬───┬────────────┘
	// │0│                      │62│                       │145│
	// ├─┴───────┬─────────┬────┼──┴──────┬─────────┬──────┼───┴─────┬──────┐
	// │Key 1 Len│   Key   │... │Key 2 Len│  Key 2  │ ...  │  Key 3  │ ...  │
	// │ 2 bytes │ N bytes │    │ 2 bytes │ N bytes │      │ 2 bytes │      │
	// └─────────┴─────────┴────┴─────────┴─────────┴──────┴─────────┴──────┘

	// We would build an `offsets` slices where each element pointers to the byte location
	// for the first key in the index slice.

	// ┌────────────────────────────────────────────────────────────────────┐
	// │                              Offsets                               │
	// ├────┬────┬────┬─────────────────────────────────────────────────────┘
	// │ 0  │ 62 │145 │
	// └────┴────┴────┘

	// Using this offset slice we can find `Key 2` by doing a binary search
	// over the offsets slice.  Instead of comparing the value in the offsets
	// (e.g. `62`), we use that as an index into the underlying index to
	// retrieve the key at position `62` and perform our comparisons with that.

	// When we have identified the correct position in the index for a given
	// key, we could perform another binary search or a linear scan.  This
	// should be fast as well since each index entry is 28 bytes and all
	// contiguous in memory.  The current implementation uses a linear scan since the
	// number of block entries is expected to be < 100 per key.

	// b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
	// slice reference
	b []byte

	// offsets contains the positions in b for each key.  It points to the 2 byte length of
	// key.
	offsets []byte

	// minKey, maxKey are the minimum and maximum (lexicographically sorted) contained in the
	// file
	minKey, maxKey []byte

	// minTime, maxTime are the minimum and maximum times contained in the file across all
	// series.
	minTime, maxTime int64

	// tombstones contains only the tombstoned keys with subset of time values deleted.  An
	// entry would exist here if a subset of the points for a key were deleted and the file
	// had not be re-compacted to remove the points on disk.
	tombstones map[string][]TimeRange
}

// TimeRange holds a min and max timestamp.
type TimeRange struct {
	Min, Max int64
}

func (t TimeRange) Overlaps(min, max int64) bool {
	return t.Min <= max && t.Max >= min
}

// NewIndirectIndex returns a new indirect index.
func NewIndirectIndex() *indirectIndex {
	return &indirectIndex{
		tombstones: make(map[string][]TimeRange),
	}
}

func (d *indirectIndex) offset(i int) int {
	if i < 0 || i+4 > len(d.offsets) {
		return -1
	}
	return int(binary.BigEndian.Uint32(d.offsets[i*4 : i*4+4]))
}

func (d *indirectIndex) Seek(key []byte) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.searchOffset(key)
}

// searchOffset searches the offsets slice for key and returns the position in
// offsets where key would exist.
func (d *indirectIndex) searchOffset(key []byte) int {
	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	i := SearchBytesFixed(d.offsets, 4, func(x []byte) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := int32(binary.BigEndian.Uint32(x))

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(d.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(d.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(d.offsets) {
		return int(i / 4)
	}

	// The key is not in the index.  i is the index where it would be inserted so return
	// a value outside our offset range.
	return int(len(d.offsets)) / 4
}

// search returns the byte position of key in the index.  If key is not
// in the index, len(index) is returned.
func (d *indirectIndex) search(key []byte) int {
	if !d.ContainsKey(key) {
		return len(d.b)
	}

	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	// TODO(sgc): this should be inlined to `indirectIndex` as it is only used here
	i := SearchBytesFixed(d.offsets, 4, func(x []byte) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := int32(binary.BigEndian.Uint32(x))

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(d.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(d.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(d.offsets) {
		ofs := binary.BigEndian.Uint32(d.offsets[i : i+4])
		_, k := readKey(d.b[ofs:])

		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at position 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(key, k) {
			return len(d.b)
		}

		return int(ofs)
	}

	// The key is not in the index.  i is the index where it would be inserted so return
	// a value outside our offset range.
	return len(d.b)
}

// ContainsKey returns true of key may exist in this index.
func (d *indirectIndex) ContainsKey(key []byte) bool {
	return bytes.Compare(key, d.minKey) >= 0 && bytes.Compare(key, d.maxKey) <= 0
}

// Entries returns all index entries for a key.
func (d *indirectIndex) Entries(key []byte) []IndexEntry {
	return d.ReadEntries(key, nil)
}

func (d *indirectIndex) readEntriesAt(ofs int, entries *[]IndexEntry) ([]byte, []IndexEntry) {
	n, k := readKey(d.b[ofs:])

	// Read and return all the entries
	ofs += n
	var ie indexEntries
	if entries != nil {
		ie.entries = *entries
	}
	if _, err := readEntries(d.b[ofs:], &ie); err != nil {
		panic(fmt.Sprintf("error reading entries: %v", err))
	}
	if entries != nil {
		*entries = ie.entries
	}
	return k, ie.entries
}

// ReadEntries returns all index entries for a key.
func (d *indirectIndex) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ofs := d.search(key)
	if ofs < len(d.b) {
		k, entries := d.readEntriesAt(ofs, entries)
		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at position 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(key, k) {
			return nil
		}

		return entries
	}

	// The key is not in the index.  i is the index where it would be inserted.
	return nil
}

// Entry returns the index entry for the specified key and timestamp.  If no entry
// matches the key an timestamp, nil is returned.
func (d *indirectIndex) Entry(key []byte, timestamp int64) *IndexEntry {
	entries := d.Entries(key)
	for _, entry := range entries {
		if entry.Contains(timestamp) {
			return &entry
		}
	}
	return nil
}

// Key returns the key in the index at the given position.
func (d *indirectIndex) Key(idx int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if idx < 0 || idx*4+4 > len(d.offsets) {
		return nil, 0, nil
	}
	ofs := binary.BigEndian.Uint32(d.offsets[idx*4 : idx*4+4])
	n, key := readKey(d.b[ofs:])

	typ := d.b[int(ofs)+n]

	var ie indexEntries
	if entries != nil {
		ie.entries = *entries
	}
	if _, err := readEntries(d.b[int(ofs)+n:], &ie); err != nil {
		return nil, 0, nil
	}
	if entries != nil {
		*entries = ie.entries
	}

	return key, typ, ie.entries
}

// KeyAt returns the key in the index at the given position.
func (d *indirectIndex) KeyAt(idx int) ([]byte, byte) {
	d.mu.RLock()

	if idx < 0 || idx*4+4 > len(d.offsets) {
		d.mu.RUnlock()
		return nil, 0
	}
	ofs := int32(binary.BigEndian.Uint32(d.offsets[idx*4 : idx*4+4]))

	n, key := readKey(d.b[ofs:])
	ofs = ofs + int32(n)
	typ := d.b[ofs]
	d.mu.RUnlock()
	return key, typ
}

// KeyCount returns the count of unique keys in the index.
func (d *indirectIndex) KeyCount() int {
	d.mu.RLock()
	n := len(d.offsets) / 4
	d.mu.RUnlock()
	return n
}

// Delete removes the given keys from the index.
func (d *indirectIndex) Delete(keys [][]byte) {
	if len(keys) == 0 {
		return
	}

	if !IsSorted(keys) {
		Sort(keys)
	}

	// Both keys and offsets are sorted.  Walk both in order and skip
	// any keys that exist in both.
	d.mu.Lock()
	start := d.searchOffset(keys[0])
	for i := start * 4; i+4 <= len(d.offsets) && len(keys) > 0; i += 4 {
		offset := binary.BigEndian.Uint32(d.offsets[i : i+4])
		_, indexKey := readKey(d.b[offset:])

		for len(keys) > 0 && bytes.Compare(keys[0], indexKey) < 0 {
			keys = keys[1:]
		}

		if len(keys) > 0 && bytes.Equal(keys[0], indexKey) {
			keys = keys[1:]
			copy(d.offsets[i:i+4], nilOffset)
		}
	}
	d.offsets = Pack(d.offsets, 4, 255)
	d.mu.Unlock()
}

// DeleteRange removes the given keys with data between minTime and maxTime from the index.
func (d *indirectIndex) DeleteRange(keys [][]byte, minTime, maxTime int64) {
	// No keys, nothing to do
	if len(keys) == 0 {
		return
	}

	if !IsSorted(keys) {
		Sort(keys)
	}

	// If we're deleting the max time range, just use tombstoning to remove the
	// key from the offsets slice
	if minTime == math.MinInt64 && maxTime == math.MaxInt64 {
		d.Delete(keys)
		return
	}

	// Is the range passed in outside of the time range for the file?
	min, max := d.TimeRange()
	if minTime > max || maxTime < min {
		return
	}

	fullKeys := make([][]byte, 0, len(keys))
	tombstones := map[string][]TimeRange{}
	var ie []IndexEntry

	for i := 0; len(keys) > 0 && i < d.KeyCount(); i++ {
		k, entries := d.readEntriesAt(d.offset(i), &ie)

		// Skip any keys that don't exist.  These are less than the current key.
		for len(keys) > 0 && bytes.Compare(keys[0], k) < 0 {
			keys = keys[1:]
		}

		// No more keys to delete, we're done.
		if len(keys) == 0 {
			break
		}

		// If the current key is greater than the index one, continue to the next
		// index key.
		if len(keys) > 0 && bytes.Compare(keys[0], k) > 0 {
			continue
		}

		// If multiple tombstones are saved for the same key
		if len(entries) == 0 {
			continue
		}

		// Is the time range passed outside of the time range we've have stored for this key?
		min, max := entries[0].MinTime, entries[len(entries)-1].MaxTime
		if minTime > max || maxTime < min {
			continue
		}

		// Does the range passed in cover every value for the key?
		if minTime <= min && maxTime >= max {
			fullKeys = append(fullKeys, keys[0])
			keys = keys[1:]
			continue
		}

		d.mu.RLock()
		existing := d.tombstones[string(k)]
		d.mu.RUnlock()

		// Append the new tombonstes to the existing ones
		newTs := append(existing, append(tombstones[string(k)], TimeRange{minTime, maxTime})...)
		fn := func(i, j int) bool {
			a, b := newTs[i], newTs[j]
			if a.Min == b.Min {
				return a.Max <= b.Max
			}
			return a.Min < b.Min
		}

		// Sort the updated tombstones if necessary
		if len(newTs) > 1 && !sort.SliceIsSorted(newTs, fn) {
			sort.Slice(newTs, fn)
		}

		tombstones[string(k)] = newTs

		// We need to see if all the tombstones end up deleting the entire series.  This
		// could happen if their is one tombstore with min,max time spanning all the block
		// time ranges or from multiple smaller tombstones the delete segments.  To detect
		// this cases, we use a window starting at the first tombstone and grow it be each
		// tombstone that is immediately adjacent to the current window or if it overlaps.
		// If there are any gaps, we abort.
		minTs, maxTs := newTs[0].Min, newTs[0].Max
		for j := 1; j < len(newTs); j++ {
			prevTs := newTs[j-1]
			ts := newTs[j]

			// Make sure all the tombstone line up for a continuous range.  We don't
			// want to have two small deletes on each edges end up causing us to
			// remove the full key.
			if prevTs.Max != ts.Min-1 && !prevTs.Overlaps(ts.Min, ts.Max) {
				minTs, maxTs = int64(math.MaxInt64), int64(math.MinInt64)
				break
			}

			if ts.Min < minTs {
				minTs = ts.Min
			}
			if ts.Max > maxTs {
				maxTs = ts.Max
			}
		}

		// If we have a fully deleted series, delete it all of it.
		if minTs <= min && maxTs >= max {
			fullKeys = append(fullKeys, keys[0])
			keys = keys[1:]
			continue
		}
	}

	// Delete all the keys that fully deleted in bulk
	if len(fullKeys) > 0 {
		d.Delete(fullKeys)
	}

	if len(tombstones) == 0 {
		return
	}

	d.mu.Lock()
	for k, v := range tombstones {
		d.tombstones[k] = v
	}
	d.mu.Unlock()
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (d *indirectIndex) TombstoneRange(key []byte) []TimeRange {
	d.mu.RLock()
	r := d.tombstones[string(key)]
	d.mu.RUnlock()
	return r
}

// Contains return true if the given key exists in the index.
func (d *indirectIndex) Contains(key []byte) bool {
	return len(d.Entries(key)) > 0
}

// ContainsValue returns true if key and time might exist in this file.
func (d *indirectIndex) ContainsValue(key []byte, timestamp int64) bool {
	entry := d.Entry(key, timestamp)
	if entry == nil {
		return false
	}

	d.mu.RLock()
	tombstones := d.tombstones[string(key)]
	d.mu.RUnlock()

	for _, t := range tombstones {
		if t.Min <= timestamp && t.Max >= timestamp {
			return false
		}
	}
	return true
}

// Type returns the block type of the values stored for the key.
func (d *indirectIndex) Type(key []byte) (byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ofs := d.search(key)
	if ofs < len(d.b) {
		n, _ := readKey(d.b[ofs:])
		ofs += n
		return d.b[ofs], nil
	}
	return 0, fmt.Errorf("key does not exist: %s", key)
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (d *indirectIndex) OverlapsTimeRange(min, max int64) bool {
	return d.minTime <= max && d.maxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (d *indirectIndex) OverlapsKeyRange(min, max []byte) bool {
	return bytes.Compare(d.minKey, max) <= 0 && bytes.Compare(d.maxKey, min) >= 0
}

// KeyRange returns the min and max keys in the index.
func (d *indirectIndex) KeyRange() ([]byte, []byte) {
	return d.minKey, d.maxKey
}

// TimeRange returns the min and max time across all keys in the index.
func (d *indirectIndex) TimeRange() (int64, int64) {
	return d.minTime, d.maxTime
}

// MarshalBinary returns a byte slice encoded version of the index.
func (d *indirectIndex) MarshalBinary() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.b, nil
}

// UnmarshalBinary populates an index from an encoded byte slice
// representation of an index.
func (d *indirectIndex) UnmarshalBinary(b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Keep a reference to the actual index bytes
	d.b = b
	if len(b) == 0 {
		return nil
	}

	//var minKey, maxKey []byte
	var minTime, maxTime int64 = math.MaxInt64, 0

	// To create our "indirect" index, we need to find the location of all the keys in
	// the raw byte slice.  The keys are listed once each (in sorted order).  Following
	// each key is a time ordered list of index entry blocks for that key.  The loop below
	// basically skips across the slice keeping track of the counter when we are at a key
	// field.
	var i int32
	var offsets []int32
	iMax := int32(len(b))
	for i < iMax {
		offsets = append(offsets, i)

		// Skip to the start of the values
		// key length value (2) + type (1) + length of key
		if i+2 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for key length value")
		}
		i += 3 + int32(binary.BigEndian.Uint16(b[i:i+2]))

		// count of index entries
		if i+indexCountSize >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for index entries count")
		}
		count := int32(binary.BigEndian.Uint16(b[i : i+indexCountSize]))
		i += indexCountSize

		// Find the min time for the block
		if i+8 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for min time")
		}
		minT := int64(binary.BigEndian.Uint64(b[i : i+8]))
		if minT < minTime {
			minTime = minT
		}

		i += (count - 1) * indexEntrySize

		// Find the max time for the block
		if i+16 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for max time")
		}
		maxT := int64(binary.BigEndian.Uint64(b[i+8 : i+16]))
		if maxT > maxTime {
			maxTime = maxT
		}

		i += indexEntrySize
	}

	firstOfs := offsets[0]
	_, key := readKey(b[firstOfs:])
	d.minKey = key

	lastOfs := offsets[len(offsets)-1]
	_, key = readKey(b[lastOfs:])
	d.maxKey = key

	d.minTime = minTime
	d.maxTime = maxTime

	var err error
	d.offsets, err = mmap(nil, 0, len(offsets)*4)
	if err != nil {
		return err
	}
	for i, v := range offsets {
		binary.BigEndian.PutUint32(d.offsets[i*4:i*4+4], uint32(v))
	}

	return nil
}

// Size returns the size of the current index in bytes.
func (d *indirectIndex) Size() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return uint32(len(d.b))
}

func (d *indirectIndex) Close() error {
	// Windows doesn't use the anonymous map for the offsets index
	if runtime.GOOS == "windows" {
		return nil
	}
	return munmap(d.offsets[:cap(d.offsets)])
}

func readKey(b []byte) (n int, key []byte) {
	// 2 byte size of key
	n, size := 2, int(binary.BigEndian.Uint16(b[:2]))

	// N byte key
	key = b[n : n+size]

	n += len(key)
	return
}

func readEntries(b []byte, entries *indexEntries) (n int, err error) {
	if len(b) < 1+indexCountSize {
		return 0, fmt.Errorf("readEntries: data too short for headers")
	}

	// 1 byte block type
	entries.Type = b[n]
	n++

	// 2 byte count of index entries
	count := int(binary.BigEndian.Uint16(b[n : n+indexCountSize]))
	n += indexCountSize

	if cap(entries.entries) < count {
		entries.entries = make([]IndexEntry, count)
	} else {
		entries.entries = entries.entries[:count]
	}

	b = b[indexCountSize+indexTypeSize:]
	for i := 0; i < len(entries.entries); i++ {
		if err = entries.entries[i].UnmarshalBinary(b); err != nil {
			return 0, fmt.Errorf("readEntries: unmarshal error: %v", err)
		}
		b = b[indexEntrySize:]
	}

	n += count * indexEntrySize

	return
}
