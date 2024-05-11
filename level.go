package equinox

import (
	"encoding/hex"
	"equinox/internel"
	"sort"
	"sync"

	"fmt"
)

// level represent tables in the same level at the LSM
type level struct {
	sync.RWMutex // tables and totalSize need update atomic
	id           int
	totalSize    int64

	// For level >= 1, tables are sorted by key ranges, which do not overlap.
	// For level 0, tables are sorted by time(aka table id), which may overlap.
	// For level 0, Compact the oldest one first
	tables []*Table
	opts   *Options
}

func NewLevel(opts *Options, id int) *level {
	return &level{
		id:   id,
		opts: opts,
	}
}

// Init just replace tables of level by input tables
func (l *level) Init(tables []*Table) {
	l.Lock()
	defer l.Unlock()

	l.totalSize = 0
	l.tables = tables
	for _, table := range tables {
		l.totalSize += table.Size()
	}

	if l.id == 0 {
		// sort by table id in ascending order, because the older table has smaller id
		sort.Slice(l.tables, func(i, j int) bool {
			return l.tables[i].ID() < l.tables[j].ID()
		})
	} else {
		// sort table by baseKeys
		sort.Slice(l.tables, func(i, j int) bool {
			return l.opts.comparator(l.tables[i].Smallest(), l.tables[j].Smallest()) < 0
		})
	}
}

// Add will add a table into this level
// Note: after add tables, sort() method should be called
func (l *level) Add(table *Table) bool {
	l.Lock()
	defer l.Unlock()

	// level0 tables is full, need to do compaction
	if l.id == 0 && len(l.tables) >= l.opts.NumLevelZeroTablesStall {
		return false
	}

	l.totalSize += table.Size()
	table.IncrRef()

	l.tables = append(l.tables, table)

	return true
}

// Sort will sort the tables in this level by smallest keys in ascending order
func (l *level) Sort() {
	// level0 don't need to sort.
	if l.id == 0 {
		return
	}

	l.RLock()
	defer l.RUnlock()

	// sort table by baseKeys
	sort.Slice(l.tables, func(i, j int) bool {
		return l.opts.comparator(l.tables[i].Smallest(), l.tables[j].Smallest()) < 0
	})
}

// Delete will delete the delTables that in this level
func (l *level) Delete(delTables []*Table) error {
	l.Lock()
	// deduplicate
	delMap := map[uint64]struct{}{}
	for _, table := range delTables {
		delMap[table.ID()] = struct{}{}
	}

	var afterDel []*Table
	for _, table := range l.tables {
		if _, found := delMap[table.ID()]; !found {
			afterDel = append(afterDel, table)
		} else {
			// found, subtract the size of deleted table
			l.totalSize -= table.Size()
		}
	}

	l.tables = afterDel
	l.Unlock()

	// may be slow, so do it after unlock
	return decrRefs(delTables)
}

// Replace will replace tables in delTables with addTables
func (l *level) Replace(delTables, addTables []*Table) error {
	if l.id == 0 {
		return nil
	}

	l.Lock()
	defer l.Unlock()
	// first delete the tables in delTables
	delMap := map[uint64]struct{}{}
	for _, table := range delTables {
		delMap[table.ID()] = struct{}{}
	}

	var newTables []*Table
	for _, table := range l.tables {
		if _, found := delMap[table.ID()]; !found {
			newTables = append(newTables, table)
		} else {
			// found, subtract the size of deleted table
			l.totalSize -= table.Size()
		}
	}

	// second add the tables in addTables
	for _, table := range addTables {
		l.totalSize += table.Size()
		table.IncrRef()
		newTables = append(newTables, table)
	}

	l.tables = newTables
	if l.id > 0 {
		// sort
		sort.Slice(l.tables, func(i, j int) bool {
			return l.opts.comparator(l.tables[i].Smallest(), l.tables[j].Smallest()) < 0
		})
	}

	return decrRefs(delTables)
}

type decrRefsFun func() error

// GetTables returns tables than contains the key,
// and decrRefsFun should be called when the tables not used.
func (l *level) GetTables(key []byte) ([]*Table, decrRefsFun) {
	l.RLock()
	defer l.RUnlock()

	numTables := len(l.tables)
	if l.id == 0 {
		// For level 0, need to check every table. just return all level0 tables,
		// because search all tables can cause the lock hold long time.
		// CAUTION: Reverse the tables. search from the newest to oldest
		res := make([]*Table, 0, numTables)
		for i := numTables - 1; i >= 0; i-- {
			res = append(res, l.tables[i])
			l.tables[i].IncrRef()
		}

		return res, func() error {
			for _, t := range res {
				if err := t.DecrRef(); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// For level >= 1, do binary search
	idx := sort.Search(numTables, func(i int) bool {
		return l.opts.comparator(l.tables[i].Biggest(), key) >= 0
	})

	if idx == numTables {
		// not found
		return nil, func() error { return nil }
	}

	res := l.tables[idx]
	res.IncrRef()
	return []*Table{res}, res.DecrRef
}

// GetValue returns the value for given key or the key after that.
func (l *level) GetValue(key []byte) (EValue, error) {
	tables, decr := l.GetTables(key)

	var value EValue
	if tables == nil {
		return value, decr()
	}

	hash := internel.Hash(ParseKey(key))
	for _, table := range tables {
		if table.DoesNotHave(hash) {
			continue
		}

		iter := table.NewIterator(false)
		defer iter.Close()

		iter.Seek(key)
		if !iter.Valid() {
			continue
		}

		matched := iter.Key()
		if SameKey(key, matched) {
			value = iter.ValueCopy()
			value.version = ParseVersion(matched)
			break
		}
	}

	return value, decr()
}

// OverlappingTables returns the tables that intersect with key range. Returns a half-interval.
func (l *level) OverlappingTables(from, to []byte) (int, int) {
	if len(from) == 0 || len(to) == 0 {
		return 0, 0
	}
	left := sort.Search(len(l.tables), func(i int) bool {
		return l.opts.comparator(from, l.tables[i].Biggest()) <= 0
	})
	right := sort.Search(len(l.tables), func(i int) bool {
		return l.opts.comparator(to, l.tables[i].Smallest()) < 0
	})

	return left, right
}

// TotalSize returns the total size of files in this level.
func (l *level) TotalSize() int64 {
	l.RLock()
	defer l.RUnlock()

	return l.totalSize
}

// NumTables returns the number of tables in this level.
func (l *level) NumTables() int {
	l.RLock()
	defer l.RUnlock()

	return len(l.tables)
}

// IsLastLevel will return true if this level is the last level(with max level id)
func (l *level) IsLastLevel() bool {
	return l.id == l.opts.MaxLevels-1
}

func (l *level) Close() error {
	l.RLock()
	defer l.RUnlock()

	var err error
	for _, table := range l.tables {
		if e := table.Close(); err != nil && err == nil {
			err = e
		}
	}

	return Wrap(err, "level.close")
}

// Validate does some sanity check on one level of data
func (l *level) Validate() error {
	if l.id == 0 {
		return nil
	}

	l.RLock()
	defer l.RUnlock()

	numTables := len(l.tables)
	for i := 1; i < numTables; i++ {
		// may be some tables deleted by other goroutines
		if i >= len(l.tables) {
			return fmt.Errorf("Level %d, j=%d numTables=%d", l.id, i, numTables)
		}

		if l.opts.comparator(l.tables[i-1].Biggest(), l.tables[i].Smallest()) >= 0 {
			return fmt.Errorf(
				"Inter: Biggest(i-1)[%d] \n%s\n vs Smallest(i)[%d]: \n%s\n: "+
					"level=%d i=%d numTables=%d",
				l.tables[i-1].ID(), hex.Dump(l.tables[i-1].Biggest()), l.tables[i].ID(),
				hex.Dump(l.tables[i].Smallest()), l.id, i, numTables)
		}

		if l.opts.comparator(l.tables[i].Smallest(), l.tables[i].Biggest()) > 0 {
			return fmt.Errorf(
				"Intra: \n%s\n vs \n%s\n: level=%d i=%d numTables=%d",
				hex.Dump(l.tables[i].Smallest()), hex.Dump(l.tables[i].Biggest()),
				l.id, i, numTables)
		}
	}

	return nil
}

func (l *level) appendIterators(iters []Iterator, option *ReadOptions) []Iterator {
	l.RLock()
	defer l.RUnlock()

	if len(l.tables) == 0 {
		return iters
	}

	if l.id == 0 {
		// add in reverse order, because the newest table is in the end of l.tables
		for i := len(l.tables) - 1; i >= 0; i-- {
			iters = append(iters, l.tables[i].NewIterator(false))
		}
	}

	return append(iters, NewTablesIterator(l.tables, false))
}

func decrRefs(tables []*Table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}

	return nil
}
