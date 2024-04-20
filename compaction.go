package equinox

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"sync"
)

type keyRange struct {
	left  []byte
	right []byte

	comparator Comparator
	inf        bool // useful when compact L0 tables
}

var infKeyRange = keyRange{inf: true}

func getKeyRange(tables ...*Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}

	comparator := tables[0].opt.comparator
	smallest := tables[0].Smallest()
	biggest := tables[1].Biggest()

	for _, table := range tables {
		if comparator(smallest, table.Smallest()) > 0 {
			smallest = table.Smallest()
		}

		if comparator(biggest, table.Biggest()) < 0 {
			biggest = table.Biggest()
		}
	}

	// pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return keyRange{
		left:       KeyWithVersion(ParseKey(smallest), math.MaxUint64),
		right:      KeyWithVersion(ParseKey(biggest), 0),
		comparator: comparator,
	}
}

func (r keyRange) Empty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

func (r keyRange) Equal(other keyRange) bool {
	return bytes.Equal(r.left, other.left) &&
		bytes.Equal(r.right, other.right) &&
		r.inf == other.inf
}

func (r keyRange) IsOverlapsWith(other keyRange) bool {
	if r.Empty() {
		return true
	}

	if other.Empty() {
		return false
	}

	if r.inf || other.inf {
		return true
	}

	if r.comparator(r.left, other.right) < 0 {
		return false
	}

	if r.comparator(r.right, other.left) < 0 {
		return false
	}

	return true
}

func (r *keyRange) MergeFrom(other keyRange) {
	if other.Empty() {
		return
	}

	if r.Empty() {
		*r = other
	}

	if len(r.left) == 0 || r.comparator(r.left, other.left) > 0 {
		r.left = other.left
	}

	if len(r.right) == 0 || r.comparator(r.right, other.right) < 0 {
		r.right = other.right
	}

	if other.inf {
		r.inf = true
	}
}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) IsOverlapsWith(other keyRange) bool {
	for _, r := range lcs.ranges {
		if r.IsOverlapsWith(other) {
			return true
		}
	}

	return false
}

func (lcs *levelCompactStatus) Remove(del keyRange) bool {
	after := []keyRange{}
	found := false
	for _, r := range lcs.ranges {
		if !r.Equal(del) {
			after = append(after, r)
		} else {
			found = true
		}
	}

	lcs.ranges = after
	return found
}

func (lcs *levelCompactStatus) String() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (cs *compactStatus) IsOverlapsWith(level int, kr keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.IsOverlapsWith(kr)
}

func (cs *compactStatus) DeleteSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()

	return cs.levels[l].delSize
}

// Delete will delete the infos from the compactStatus when the compation is completed.
func (cs *compactStatus) Delete(ci compactInfo) {
	cs.Lock()
	defer cs.Unlock()

	curr := cs.levels[ci.curr.id]
	next := cs.levels[ci.next.id]

	curr.delSize -= ci.size
	found := curr.Remove(ci.currRange)

	if ci.curr != ci.next && !ci.nextRange.Empty() {
		found = next.Remove(ci.nextRange) && found
	}

	if !found {
		currR := ci.currRange
		nextR := ci.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", currR, ci.curr.id)
		fmt.Printf("This Level:\n%s\n", curr)
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", nextR, ci.next.id)
		fmt.Printf("Next Level:\n%s\n", next)
		log.Fatal("keyRange not found")
	}
}

// Check will check whether we can run this compactDef. That it doesn't overlap with any
// other running compaction. If it can be run, it would update infos in the compactStatus state.
func (cs *compactStatus) Check(ci compactInfo) bool {
	cs.Lock()
	defer cs.Unlock()

	curr := cs.levels[ci.curr.id]
	next := cs.levels[ci.next.id]

	if curr.IsOverlapsWith(ci.currRange) {
		return false
	}

	if next.IsOverlapsWith(ci.nextRange) {
		return false
	}

	curr.ranges = append(curr.ranges, ci.currRange)
	next.ranges = append(next.ranges, ci.nextRange)
	curr.delSize += ci.size

	tables := ci.tables()
	for _, table := range tables {
		cs.tables[table.id] = struct{}{}
	}

	return true
}

type dynamicLevelSize struct {
	baseLevel  int
	targetSize []int64
	fileSize   []int64
}

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	dls          dynamicLevelSize
}

type compactInfo struct {
	compactId int

	cp   compactionPriority
	curr *level
	next *level

	fromTables []*Table
	toTables   []*Table

	currRange keyRange
	nextRange keyRange
	splits    []keyRange

	size         int64
	dropPrefixes [][]byte
}

func (ci *compactInfo) lock() {
	ci.curr.RLock()
	ci.next.RLock()
}

func (ci *compactInfo) unlock() {
	ci.curr.RUnlock()
	ci.next.RUnlock()
}

func (ci *compactInfo) tables() []*Table {
	ts := make([]*Table, 0, len(ci.fromTables)+len(ci.toTables))
	ts = append(ts, ci.fromTables...)
	ts = append(ts, ci.toTables...)

	return ts
}
