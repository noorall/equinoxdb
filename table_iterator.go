package equinox

import (
	"bytes"
	"io"
	"sort"
)

// TableIterator uses to iterate all entries int the table than iterator belong to
type TableIterator struct {
	table *Table
	biter *blockIterator

	blockId int
	err     error
	reverse bool

	comparator Comparator
	checksum   bool
}

func (t *Table) NewIterator(reverse bool) *TableIterator {
	t.IncrRef()

	return &TableIterator{table: t,
		reverse:    reverse,
		comparator: t.opt.comparator,
		checksum:   true,
	}
}

// DisableChecksum will disable checksum when iterator access the file.
func (iter *TableIterator) DisableChecksum() {
	iter.checksum = false
}

// Seek return the first entry that is >= input key from start (or <= input key if reverse enable)
func (iter *TableIterator) Seek(key []byte) {
	iter.seekFrom(key, restart)
	if iter.reverse && !SameKey(iter.Key(), key) {
		iter._prev()
	}
}

// Seek return the first entry that is >= input key from pos (may be start or last pos)
func (iter *TableIterator) seekFrom(key []byte, mode seekMode) {
	iter.err = nil
	if mode == restart {
		iter.reset()
	}

	found := sort.Search(iter.table.BlockCount(), func(i int) bool {
		currKey := iter.table.index.baseKeys[i]

		return iter.comparator(currKey, key) > 0
	})

	if found == 0 {
		// the smallest key in table is greater than input key, just like SeekToFirst
		iter.seekInBlock(0, key)
		return
	}

	// block[found].smallest is > key.
	// Since found>0, we know block[found-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[found-1] is strictly < key. In this case, we should go to the first
	//    element of block[found].
	// 2) Some element in block[found-1] is >= key. We should go to that element.
	iter.seekInBlock(found-1, key)
	if iter.err == io.EOF {
		// Case 1. Need to visit block[found].
		if found == iter.table.BlockCount() {
			// If found == iter.table.BlockCount() then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[found].smallest is > key. This is essentially a block[found].SeekToFirst.
		iter.seekInBlock(found, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[found-1].
}

// SeekToFirst get the first entry(with smallest key) in the table (or last entry if reverse enable).
func (iter *TableIterator) SeekToFirst() {
	if iter.reverse {
		iter._seekToLast()
	} else {
		iter._seekToFirst()
	}
}

func (iter *TableIterator) _seekToFirst() {
	iter.err = nil
	iter.readBlockById(0)
	if iter.err != nil {
		return
	}
	iter.biter.SeekToFirst()
}

// SeekToLast get the last entry(with biggest key) in the table (or first entry if reverse enable).
func (iter *TableIterator) SeekToLast() {
	if iter.reverse {
		iter._seekToFirst()
	} else {
		iter._seekToLast()
	}
}

func (iter *TableIterator) _seekToLast() {
	iter.err = nil
	iter.readBlockById(iter.table.BlockCount() - 1)
	if iter.err != nil {
		return
	}
	iter.biter.SeekToLast()
}

// Next get next entry in the current block (or prev entry if reverse enable).
func (iter *TableIterator) Next() {
	if iter.reverse {
		iter._prev()
	} else {
		iter._next()
	}
}

func (iter *TableIterator) _next() {
	iter.err = nil

	// no more block
	if iter.blockId >= iter.table.BlockCount() {
		iter.err = io.EOF
		return
	}

	// invalid or first read
	if iter.biter == nil {
		iter.readBlockById(iter.blockId)
		if iter.err != nil {
			return
		}

		iter.biter.SeekToFirst()
		iter.err = iter.biter.err
		return
	}

	// if reach the end of current block, get the next block
	iter.biter.Next()
	if !iter.biter.Valid() {
		iter.blockId++
		iter.biter = nil
		iter._next()
	}
}

// Prev get prev entry in the current block (or next entry if reverse enable)
func (iter *TableIterator) Prev() {
	if iter.reverse {
		iter._next()
	} else {
		iter._prev()
	}
}

func (iter *TableIterator) _prev() {
	iter.err = nil

	// no more block
	if iter.blockId < 0 {
		iter.err = io.EOF
		return
	}

	// invalid or first read, read from the last entry of first block
	if iter.biter == nil {
		iter.readBlockById(iter.blockId)
		if iter.err != nil {
			return
		}

		iter.biter.SeekToLast()
		iter.err = iter.biter.Error()
		return
	}

	// if reach the begin of current block, get the next block
	iter.biter.Prev()
	if !iter.biter.Valid() {
		iter.blockId--
		iter.biter = nil
		iter._prev()
	}
}

// Returns the key with version.
func (iter *TableIterator) Key() []byte {
	return iter.biter.Key()
}

// Returns the value with meta
func (iter *TableIterator) Value() EValue {
	return iter.biter.Value()
}

// Returns the copy value with meta
func (iter *TableIterator) ValueCopy() (ret EValue) {
	dst := make([]byte, len(iter.biter.value))
	copy(dst, iter.biter.value)
	ret.Decode(dst)
	return
}

// Close closes the iterator (and it must be called).
func (iter *TableIterator) Close() error {
	if iter.biter != nil {
		iter.biter.Close()
	}

	return iter.table.DecrRef()
}

func (iter *TableIterator) Valid() bool {
	return iter.err == nil
}

func (iter *TableIterator) seekInBlock(bid int, key []byte) {
	iter.readBlockById(bid)
	if iter.err != nil {
		return
	}
	iter.biter.Seek(key)
	iter.err = iter.biter.Error()
}

func (iter *TableIterator) readBlockById(id int) {
	if iter.blockId != id || iter.biter == nil {
		iter.blockId = id
		block, err := iter.table.getBlockAt(id, iter.checksum)
		if err != nil {
			iter.err = err
			return
		}

		iter.err = nil
		if iter.biter != nil {
			iter.biter.Close()
		}

		iter.biter = NewBlockIterator(iter.table.opt, block, int(iter.table.ID()), id)
	}
}

func (iter *TableIterator) reset() {
	iter.blockId = 0
	iter.err = nil
}

// TablesIterator uses to iterate over all the entries in tables than the iterator has
type TablesIterator struct {
	currIter *TableIterator
	iters    []*TableIterator
	tables   []*Table

	tableId    int
	reverse    bool
	comparator Comparator
}

func NewTablesIterator(tables []*Table, reverse bool) *TablesIterator {
	if len(tables) == 0 {
		return nil
	}

	iters := make([]*TableIterator, len(tables))

	for i := 0; i < len(tables); i++ {
		tables[i].IncrRef()
	}

	return &TablesIterator{
		iters:      iters,
		tables:     tables,
		tableId:    -1,
		reverse:    reverse,
		comparator: tables[0].opt.comparator,
	}
}

// Seek find the first entry's key >= input key, 0r <= input key if reverse enable.
func (iter *TablesIterator) Seek(key []byte) {
	n := len(iter.tables)

	var found int
	if iter.reverse {
		found = n - 1 - sort.Search(n, func(i int) bool {
			return iter.comparator(iter.tables[n-1-i].Smallest(), key) <= 0
		})
	} else {
		found = sort.Search(n, func(i int) bool {
			return iter.comparator(iter.tables[i].Biggest(), key) >= 0
		})
	}

	if found == n || found < 0 {
		// not found, change status to invalid
		iter.getTableIteratorById(-1)
		return
	}

	iter.getTableIteratorById(found)
	iter.currIter.Seek(key)
}

// SeekToFirst get the first entry(with smallest key) in the tables, or last entry if reverse enable.
func (iter *TablesIterator) SeekToFirst() {
	if iter.reverse {
		iter.getTableIteratorById(len(iter.tables) - 1)
	} else {
		iter.getTableIteratorById(0)
	}

	iter.currIter.SeekToFirst()
}

// SeekToLast get the last entry(with biggest key) in the table, or first entry if reverse enable.
func (iter *TablesIterator) SeekToLast() {
	if iter.reverse {
		iter.getTableIteratorById(0)
	} else {
		iter.getTableIteratorById(len(iter.tables) - 1)
	}

	iter.currIter.SeekToLast()
}

// Next get next entry in the tables, or prev entry if reverse enable.
func (iter *TablesIterator) Next() {
	iter.currIter.Next()
	if iter.currIter.Valid() {
		return
	}

	// find the next not empty table
	for {
		if iter.reverse {
			iter.getTableIteratorById(iter.tableId - 1)
		} else {
			iter.getTableIteratorById(iter.tableId + 1)
		}

		if !iter.Valid() {
			// can not find any table can read
			return
		}

		iter.currIter.SeekToFirst()
		if iter.currIter.Valid() {
			// find the table
			return
		}
	}
}

// Prev get prev entry in the tables, or next entry if reverse enable.
func (iter *TablesIterator) Prev() {
	iter.currIter.Prev()
	if iter.currIter.Valid() {
		return
	}

	// find the next not empty table
	for {
		if iter.reverse {
			iter.getTableIteratorById(iter.tableId + 1)
		} else {
			iter.getTableIteratorById(iter.tableId - 1)
		}

		if !iter.Valid() {
			// can not find any table can read
			return
		}

		iter.currIter.SeekToLast()
		if iter.currIter.Valid() {
			// find the table
			return
		}
	}
}

func (iter *TablesIterator) Key() []byte {
	return iter.currIter.Key()
}

func (iter *TablesIterator) Value() EValue {
	return iter.currIter.Value()
}

func (iter *TablesIterator) Valid() bool {
	return iter.currIter != nil && iter.currIter.Valid()
}

func (iter *TablesIterator) Close() error {
	if iter == nil {
		return nil
	}
	for _, table := range iter.tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}

	for _, it := range iter.iters {
		if it == nil {
			continue
		}

		if err := it.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (iter *TablesIterator) getTableIteratorById(tableId int) {
	if tableId < 0 || tableId >= len(iter.tables) {
		iter.currIter = nil
		return
	}

	iter.tableId = tableId
	if iter.iters[tableId] == nil {
		iter.iters[tableId] = iter.tables[tableId].NewIterator(iter.reverse)
	}

	iter.currIter = iter.iters[tableId]
}

type mergeNode struct {
	key   []byte
	iter  Iterator
	valid bool

	// optimize, Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	mergeIter  *TablesMergeIterator
	tablesIter *TablesIterator
}

// TablesMergeIterator merge different tables through TableIterator or TablesIterator
type TablesMergeIterator struct {
	left  mergeNode
	right mergeNode
	small *mergeNode

	curKey     []byte
	reverse    bool
	comparator Comparator
}

func (n *mergeNode) init(iter Iterator) {
	n.iter = iter

	n.mergeIter, _ = iter.(*TablesMergeIterator)
	n.tablesIter, _ = iter.(*TablesIterator)
}

func (n *mergeNode) getKey() {
	switch {
	case n.mergeIter != nil:
		n.valid = n.mergeIter.small.valid
		if n.valid {
			n.key = n.mergeIter.small.key
		}
	case n.tablesIter != nil:
		n.valid = n.tablesIter.Valid()
		if n.valid {
			n.key = n.tablesIter.Key()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.key = n.iter.Key()
		}
	}
}

func (n *mergeNode) next() {
	switch {
	case n.mergeIter != nil:
		n.mergeIter.Next()
	case n.tablesIter != nil:
		n.tablesIter.Next()
	default:
		n.iter.Next()
	}

	n.getKey()
}

func (n *mergeNode) prev() {
	switch {
	case n.mergeIter != nil:
		n.mergeIter.Prev()
	case n.tablesIter != nil:
		n.tablesIter.Prev()
	default:
		n.iter.Prev()
	}

	n.getKey()
}

func (n *mergeNode) seek(key []byte) {
	n.iter.Seek(key)
	n.getKey()
}

func (n *mergeNode) seekToFirst() {
	n.iter.SeekToFirst()
	n.getKey()
}

func (n *mergeNode) seekToLast() {
	n.iter.SeekToLast()
	n.getKey()
}

// NewTablesMergeIterator creates a merge iterator.
func NewTablesMergeIterator(opt *Options, iters []Iterator, reverse bool) Iterator {
	switch len(iters) {
	case 0:
		return nil
	case 1:
		return iters[0]
	case 2:
		m := &TablesMergeIterator{
			reverse: reverse,
		}

		m.left.init(iters[0])
		m.right.init(iters[1])

		// m.keepOrder() will make this correct
		m.small = &m.left
		m.comparator = opt.comparator
		return m
	}

	mid := len(iters) / 2
	// create recursively
	return NewTablesMergeIterator(
		opt,
		[]Iterator{
			NewTablesMergeIterator(opt, iters[:mid], reverse),
			NewTablesMergeIterator(opt, iters[mid:], reverse),
		},
		reverse,
	)
}

// Seek get entry with key >= given key (or key <= given key if reverse enable).
func (m *TablesMergeIterator) Seek(key []byte) {
	m.left.seek(key)
	m.right.seek(key)
	m.keepOrder()
	m.getCurrentKey()
}

// SeekToFirst get first entry (or last entry if reverse enable)
func (m *TablesMergeIterator) SeekToFirst() {
	m.left.seekToFirst()
	m.right.seekToFirst()

	m.keepOrder()
	m.getCurrentKey()
}

// SeekToLast get last entry  (or first entry if reverse enable)
func (m *TablesMergeIterator) SeekToLast() {
	m.left.seekToLast()
	m.right.seekToLast()

	m.keepOrder()
	m.getCurrentKey()
}

// Next returns the next entry (or prev entry if reverse enable).
// If it is the same as the current key, ignore it.
func (m *TablesMergeIterator) Next() {
	for m.Valid() {
		// until find the different key
		if !bytes.Equal(m.small.key, m.curKey) {
			break
		}

		m.small.next()
		m.keepOrder()
	}

	m.getCurrentKey()
}

// Next returns the prev entry (or prev entry if reverse enable).
// If it is the same as the current key, ignore it.
func (m *TablesMergeIterator) Prev() {
	for m.Valid() {
		// until find the different key
		if !bytes.Equal(m.small.key, m.curKey) {
			break
		}

		m.small.prev()
		m.keepOrder()
	}

	m.getCurrentKey()
}

// Valid returns whether the TablesMergeIterator is at a valid entry.
func (m *TablesMergeIterator) Valid() bool {
	return m.small.valid
}

// Key returns the key associated with the current iterator.
func (m *TablesMergeIterator) Key() []byte {
	return m.small.key
}

// Value returns the value associated with the iterator.
func (m *TablesMergeIterator) Value() EValue {
	return m.small.iter.Value()
}

// Close all the iterators
func (m *TablesMergeIterator) Close() error {
	lerr := m.left.iter.Close()

	var rerr error

	if m.right.iter != nil {
		rerr = m.right.iter.Close()
	}

	if lerr != nil {
		return Wrap(lerr, "TablesMergeIterator left")
	}

	return Wrap(rerr, "TablesMergeIterator right")
}

func (m *TablesMergeIterator) getCurrentKey() {
	// deep copy
	m.curKey = append(m.curKey[:0], m.small.key...)
}

func (m *TablesMergeIterator) bigger() *mergeNode {
	if m.small == &m.left {
		return &m.right
	}

	return &m.left
}

func (m *TablesMergeIterator) swap() {
	if m.small == &m.left {
		m.small = &m.right
	} else if m.small == &m.right {
		m.small = &m.left
	}
}

func (m *TablesMergeIterator) keepOrder() {
	// if the bigger is invalid, than the iterator is also invalid
	if !m.bigger().valid {
		return
	}

	// if the smaller is invalid, than change the small to the bigger, make the iterator keep valid
	if !m.small.valid {
		m.swap()
		return
	}

	cmp := m.comparator(m.small.key, m.bigger().key)
	switch {
	case cmp == 0:
		// both the keys are equal, so make right iterator ahead.
		// Become the bigger one ( or the smaller one if reverse enable).
		m.right.next()

		if &m.right == m.small {
			m.swap()
		}
		return
	case cmp < 0:
		// the small key is less than bigger's
		if m.reverse {
			// if reverse enable, than need make the small points to the bigger
			m.swap()
		}
		// else do nothing, the small already points to the smallest
		return
	default:
		// the small key is great than bigger's
		if !m.reverse {
			// if reverse disable, than need make the small points to the bigger
			m.swap()
		}
		// else do nothing, the small already points to the biggest
	}
}
