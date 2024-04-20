package equinox

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"sync/atomic"

	"fmt"
)

var emptyValue = []byte{}
var wbOption *Options = nil

type WriteBatch struct {
	entries []*entry
	sz      uint64
	sync    bool // write sync or not
}

func NewWriteBatch(db DB) *WriteBatch {
	if wbOption == nil {
		option := db.GetOption()
		wbOption = &option
	}
	return &WriteBatch{
		entries: make([]*entry, 0),
		sz:      0,
		sync:    false,
	}
}

func (b *WriteBatch) Put(key, value []byte) {
	e := &entry{
		key:   key,
		value: value,
		rtype: Value,
	}

	b.entries = append(b.entries, e)
	b.sz += uint64(e.estimateSize(uint32(wbOption.ValueThreshold)) + 8)
}

func (b *WriteBatch) Delete(key []byte) {
	e := &entry{
		key:   key,
		value: emptyValue,
		rtype: Delete,
	}

	b.entries = append(b.entries, e)
	b.sz += uint64(e.estimateSize(uint32(wbOption.ValueThreshold)) + 8)
}

func (b *WriteBatch) Append(other *WriteBatch) {
	b.entries = append(b.entries, other.entries...)
	b.sz += other.sz
}

func (b *WriteBatch) Clear() {
	b.entries = b.entries[:0]
	b.sz = 0
	b.sync = false
}

func (b *WriteBatch) Count() int {
	return len(b.entries)
}

func (b *WriteBatch) ApproximateSize() uint64 {
	return b.sz
}

func (b *WriteBatch) Empty() bool {
	return len(b.entries) == 0
}

func (b *WriteBatch) FullWith(key, value []byte) bool {
	e := &entry{
		key:   key,
		value: value,
		rtype: Value, // it doesn't matter
	}

	count := b.Count() + 1
	size := b.sz + uint64(e.estimateSize(uint32(wbOption.ValueThreshold))+8)

	return count >= wbOption.maxBatchCount || size >= uint64(wbOption.maxBatchSize)
}

func (b *WriteBatch) Validate() error {
	const maxKeySize = 65536 // determined by fileEntryHeader

	if b == nil || b.Empty() {
		return ErrEmptyBatch
	}

	if b.Count() >= wbOption.maxBatchCount ||
		b.ApproximateSize() >= uint64(wbOption.maxBatchSize) {
		return ErrBatchTooBig
	}

	for _, e := range b.entries {
		switch {
		case len(e.key) == 0:
			return ErrEmptyKey
		case len(e.key) > maxKeySize:
			return exceedsSize("Key", maxKeySize, e.key)
		case len(e.value) > wbOption.ValueLogFileSize:
			return exceedsSize("Value", wbOption.ValueLogFileSize, e.value)
		}
	}

	return nil
}

func exceedsSize(prefix string, max int, key []byte) error {
	return fmt.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

type writeBatchInternel struct {
	WriteBatch
	ptrs []valPtr

	wg  sync.WaitGroup
	err error

	ref     int32
	version uint64
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return new(writeBatchInternel)
	},
}

func (b *writeBatchInternel) Fill(wb *WriteBatch) {
	b.entries = make([]*entry, len(wb.entries))
	b.sz = wb.sz

	for i, e := range wb.entries {
		b.entries[i] = &entry{
			key:   KeyWithVersion(e.key, b.version),
			value: e.value,
			rtype: e.rtype,
		}

		// with length of version
		b.sz += 8

		// every entry has a unique version number
		b.version++
	}
}

func (b *writeBatchInternel) IncrRef() {
	atomic.AddInt32(&b.ref, 1)
}

func (b *writeBatchInternel) DecrRef() {
	nRef := atomic.AddInt32(&b.ref, -1)
	if nRef > 0 {
		return
	}
	b.entries = nil
	batchPool.Put(b)
}

func (b *writeBatchInternel) Wait() error {
	b.wg.Wait()
	err := b.err
	b.DecrRef() // DecrRef after writing to DB.
	return err
}

func (b *writeBatchInternel) Reset() {
	b.entries = b.entries[:0]
	b.ptrs = b.ptrs[:0]
	b.wg = sync.WaitGroup{}
	b.ref = 0
	b.sz = 0
}

func (b *writeBatchInternel) setVersion(s uint64) {
	b.version = s
}

func KeyWithVersion(key []byte, version uint64) []byte {
	res := make([]byte, len(key)+8)
	copy(res, key)
	binary.BigEndian.PutUint64(res[len(key):], version)

	return res
}

func ParseVersion(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}

	return binary.BigEndian.Uint64(key[len(key)-8:])
}

func ParseKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	return key[:len(key)-8]
}

func SameKey(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}

	return bytes.Equal(ParseKey(key1), ParseKey(key2))
}

// first compare keys by increasing order, then compare sequence number by decreasing order
func CompareKeys(a, b []byte) int {
	if cmp := bytes.Compare(ParseKey(a), ParseKey(b)); cmp != 0 {
		return cmp
	}

	return bytes.Compare(b[len(a)-8:], a[len(b)-8:])
}
