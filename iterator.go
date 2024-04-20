package equinox

import (
	"bytes"
	"equinox/internel"
	"hash/crc32"
)

type Iterator interface {
	Seek(key []byte)
	SeekToFirst()
	SeekToLast()
	Next()
	Prev()
	Key() []byte
	Value() EValue
	Valid() bool
	Close() error
}

type DBIterator struct {
	db     *DBImpl
	option *ReadOptions

	iters   Iterator
	currKey []byte
	item    *Item

	closed     bool
	valid      bool
	reverse    bool
	maxVersion uint64

	Err error
}

func NewDBIterator(db *DBImpl, option *ReadOptions, iters Iterator, version uint64) *DBIterator {
	db.vlog.incrIteratorCount()
	mx := version
	if option.Snapshot != nil {
		mx = option.Snapshot.version
	}

	return &DBIterator{
		db:         db,
		option:     option,
		iters:      iters,
		maxVersion: mx,
		valid:      true,
	}
}

// Seek find the first valid record that key is same with input key.
// That may change iterator to invalid if there is not have any key same with input key.
func (it *DBIterator) Seek(key []byte) {
	it.reverse = false
	it.item = nil
	it.currKey = KeyWithVersion(key, it.maxVersion)
	it.iters.Seek(it.currKey)
	if it.iters.Valid() {
		it.findNextUserEntry(false, &it.currKey)
	} else {
		it.valid = false
	}
}

// SeekToFirst move iterator to the begin of db, then find the first valid record.
// That may change iterator to invalid if there is not have any valid record.
func (it *DBIterator) SeekToFirst() {
	it.reverse = false
	it.item = nil

	it.iters.SeekToFirst()
	if it.iters.Valid() {
		it.findNextUserEntry(false, &it.currKey)
	} else {
		it.valid = false
	}
}

// SeekToLast move iterator to the end of db, then find the first valid record.
// That may change iterator to invalid if there is not have any valid record.
func (it *DBIterator) SeekToLast() {
	it.reverse = true
	it.item = nil

	it.iters.SeekToLast()

	it.findPrevUserEntry()
}

// Prev move iterator forward until find any valid record,
// and that may change iterator to invalid if there is not have any valid record.
func (it *DBIterator) Next() {
	if !it.valid {
		return
	}

	if it.reverse {
		it.reverse = false

		if !it.iters.Valid() {
			it.iters.SeekToFirst()
		} else {
			it.iters.Next()
		}

		if !it.iters.Valid() {
			it.valid = false
			it.currKey = it.currKey[:0]
			return
		}
	} else {
		item := it.GetItem()
		it.currKey = safeCopy(it.currKey, item.key)

		it.iters.Next()
		if !it.iters.Valid() {
			it.valid = false
			it.currKey = it.currKey[:0]
		}
	}

	it.findNextUserEntry(true, &it.currKey)
}

// Prev move iterator backward until find any valid record,
// and that may change iterator to invalid if there is not have any valid record.
func (it *DBIterator) Prev() {
	if !it.valid {
		return
	}

	if !it.reverse {
		item := it.GetItem()
		it.currKey = safeCopy(it.currKey, it.item.key)

		for {
			it.iters.Prev()

			if !it.iters.Valid() {
				it.valid = false
				it.currKey = it.currKey[:0]
				it.item = nil
				return
			}

			if it.option.AllVersion || !bytes.Equal(item.key, it.currKey) {
				break
			}

			item = it.GetItem()
		}

		it.reverse = true
	} else if it.option.AllVersion {
		it.iters.Prev()
	}

	it.findPrevUserEntry()
}

// Key returns current key from db with this iterator.
func (it *DBIterator) Key() []byte {
	if !it.valid {
		return nil
	}

	if it.reverse {
		return it.currKey
	} else {
		it.item = it.GetItem()
		return it.item.key
	}
}

// Value returns current value from db with this iterator
func (it *DBIterator) Value() EValue {
	if !it.valid {
		return EValue{}
	}

	if !it.reverse {
		it.item = it.GetItem()
	}

	if err := it.readFromVlog(it.item); err != nil {
		return EValue{}
	}

	return EValue{Value: it.item.value, Meta: it.item.rtype, version: it.item.version}
}

func (it *DBIterator) readFromVlog(item *Item) error {
	if item.rtype&ValPtr == 0 {
		return nil
	}

	entry, err := it.db.vlog.Read(it.option, item.vp)
	if err != nil {
		it.db.option.Logger.Errorf("Unable to read from vlog: Key: %v, Version : %v, meta: %v"+
			" Error: %v", item.key, item.version, item.rtype, err)
		it.Err = err
		return err
	}

	item.value = safeCopy(item.value, entry.value)
	return nil
}

// Valid returns true if this iterator can use, otherwise false
func (it *DBIterator) Valid() bool {
	return it.valid
}

// Close will close this iterator, if it not closed. Otherwise, do nothing
func (it *DBIterator) Close() error {
	if it.closed {
		return it.Err
	}

	it.valid = false
	it.closed = true
	it.Err = it.iters.Close()

	if err := it.db.vlog.decrIteratorCount(); it.Err == nil {
		it.Err = err
	}

	return it.Err
}

// findNextUserEntry get the next entry that not deleted
// and its version is max but not greater than maxVersion
func (it *DBIterator) findNextUserEntry(skipping bool, skip *[]byte) {
	for ; it.iters.Valid(); it.iters.Next() {
		item := it.GetItem()
		if item != nil && item.version <= it.maxVersion {
			if it.option.AllVersion {
				return
			}

			switch item.rtype &^ ValPtr {
			case Delete:
				skipping = true
				*skip = safeCopy(*skip, item.key)
			case Value:
				if !skipping || !bytes.Equal(item.key, *skip) {
					// not the skip key
					it.valid = true
					it.currKey = it.currKey[:0]
					return
				}
			}
		}
	}

	it.valid = false
	it.currKey = it.currKey[:0]
}

// findPrevUserEntry get the prev entry that not deleted
// and its version is max but not greater than maxVersion
func (it *DBIterator) findPrevUserEntry() {
	t := Delete
	for ; it.iters.Valid(); it.iters.Prev() {
		item := it.GetItem()
		if item != nil && item.version <= it.maxVersion {
			if it.option.AllVersion {
				it.currKey = safeCopy(it.currKey, item.key)
				it.item = item
				return
			}

			if t != Delete && !bytes.Equal(item.key, it.currKey) {
				break
			}

			t = item.rtype & Delete
			if t == Delete {
				it.currKey = it.currKey[:0]
				it.item = nil
			} else {
				it.currKey = safeCopy(it.currKey, item.key)
				it.item = item
			}
		}
	}

	if t == Delete {
		// no more records
		it.valid = false
		it.currKey = it.currKey[:0]
		it.item = nil

		it.reverse = false
	} else {
		it.valid = true
	}
}

type Item struct {
	key     []byte
	value   []byte
	rtype   byte
	version uint64

	vp valPtr
}

func (item *Item) Key() []byte {
	return item.key
}

func (item *Item) KeyCopy(dst []byte) []byte {
	return safeCopy(dst, item.key)
}

func (item *Item) KeySize() int64 {
	return int64(len(item.key))
}

func (item *Item) Value() []byte {
	return item.value
}

func (item *Item) ValueCopy(dst []byte) []byte {
	return safeCopy(dst, item.value)
}

func (item *Item) ValueSize() int64 {
	if item.InValueLog() {
		klen := int64(len(item.key) + 8)
		// 7 bytes are for the approximate length of the header.
		return int64(item.vp.len) - klen - 7 - crc32.Size
	}

	return int64(len(item.value))
}

func (item *Item) Version() uint64 {
	return item.version
}

func (item *Item) Deleted() bool {
	return item.rtype&Delete == Delete
}

func (item *Item) InValueLog() bool {
	return item.rtype&ValPtr == ValPtr
}

func (it *DBIterator) GetItem() *Item {
	if !it.valid || !it.iters.Valid() {
		return nil
	}

	var item Item

	rawKey := it.iters.Key()
	item.key = ParseKey(rawKey)
	item.version = ParseVersion(rawKey)

	rawVal := it.iters.Value()
	item.rtype = rawVal.Meta
	if rawVal.Meta&ValPtr == ValPtr {
		var vp valPtr
		vp.Decode(rawVal.Value)
		item.vp = vp
	} else {
		item.value = safeCopy(item.value, rawVal.Value)
	}

	return &item
}

func safeCopy(dst []byte, src []byte) []byte {
	return append(dst[:0], src...)
}

type memTableIterator struct {
	iter *internel.Iterator
}

func (mi *memTableIterator) Key() []byte {
	return mi.iter.Key()
}

func (mi *memTableIterator) Value() EValue {
	var ev EValue
	ev.Decode(mi.iter.Value())

	return ev
}

func (mi *memTableIterator) Valid() bool {
	return mi.iter.Valid()
}

func (mi *memTableIterator) Next() {
	mi.iter.Next()
}

func (mi *memTableIterator) Prev() {
	mi.iter.Prev()
}

func (mi *memTableIterator) Seek(target []byte) {
	mi.iter.Seek(target)
}

func (mi *memTableIterator) SeekToFirst() {
	mi.iter.SeekToFirst()
}

func (mi *memTableIterator) SeekToLast() {
	mi.iter.SeekToLast()
}

func (mi *memTableIterator) Close() error {
	return mi.iter.Close()
}
