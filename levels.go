package equinox

import (
	"bytes"
	"equinox/internel"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type levels struct {
	nextId     uint64 // atomic
	l0stallMs  int64  // atomic
	maxVersion uint64 // atomic

	ls      []*level
	cstatus compactStatus

	opts      *Options
	vlog      *valueLog
	manifest  *manifestFile
	snapshots *snapshotList
}

func NewLevels(opts *Options, mf *Manifest, vlog *valueLog,
	mff *manifestFile, snapshots *snapshotList) (*levels, error) {
	s := &levels{
		ls:        make([]*level, opts.MaxLevels),
		opts:      opts,
		vlog:      vlog,
		manifest:  mff,
		snapshots: snapshots,
	}

	s.cstatus.tables = make(map[uint64]struct{})
	s.cstatus.levels = make([]*levelCompactStatus, opts.MaxLevels)

	for i := 0; i < opts.MaxLevels; i++ {
		s.ls[i] = NewLevel(opts, i)
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	if err := checkTablesByManifest(opts, mf); err != nil {
		return nil, err
	}

	// create reasonable number of goroutines can make the speed of load tables up
	tables, maxFileId, err := loadTables(opts, mf)
	if err != nil {
		return nil, err
	}

	s.nextId = maxFileId + 1
	for i, tbls := range tables {
		s.ls[i].Init(tbls)
	}

	// Make sure key ranges do not overlap etc.
	if err := s.Validate(); err != nil {
		_ = s.Close()
		return nil, Wrap(err, "Levels validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	if err := syncDir(opts.Dir); err != nil {
		_ = s.Close()
		return nil, err
	}

	return s, nil
}

func (ls *levels) updateMaxVersion(version uint64) uint64 {
	oldVersion := ls.MaxVersion()

	for !atomic.CompareAndSwapUint64(&ls.maxVersion, oldVersion, oldVersion+version) {
		oldVersion = ls.MaxVersion()
	}

	return oldVersion
}

func (ls *levels) MaxVersion() uint64 {
	return atomic.LoadUint64(&ls.maxVersion)
}

// GetValue searches the value for a given key in all the levels of the LSM tree. It returns
// key version <= the expected version (with the key). If not found, it returns an empty.
func (ls *levels) GetValue(key []byte, startLevel int) (EValue, error) {
	res := EValue{}
	for _, level := range ls.ls {
		if level.id < startLevel {
			continue
		}

		ev, err := level.GetValue(key)
		if err != nil {
			return EValue{}, Wrapf(err, "get key: %q", key)
		}

		// empty value
		if ev.Value == nil && ev.Meta == 0 {
			continue
		}

		res = ev
		break
	}

	return res, nil
}

// LastLevel return the last level of levels
func (ls *levels) LastLevel() *level {
	return ls.ls[len(ls.ls)-1]
}

// Validate will check it validate for all levels
func (ls *levels) Validate() error {
	for _, l := range ls.ls {
		if err := l.Validate(); err != nil {
			return Wrap(err, "Levels")
		}
	}
	return nil
}

// KeySplits Returns the sorted list of splits for all the levels and tables based
// on the block offsets.
func (ls *levels) KeySplits(numPerTable int, prefix []byte) []string {
	splits := []string{}

	for _, level := range ls.ls {
		level.RLock()

		for _, t := range level.tables {
			tSplits := t.KeySplits(numPerTable, prefix)
			splits = append(splits, tSplits...)
		}

		level.RUnlock()
	}

	sort.Strings(splits)

	return splits
}

// AddLevel0Table will add the table into level 0 if the level0 isn't full.
// Otherwise it will try some times until add successfully.
func (ls *levels) AddLevel0Table(table *Table) error {
	changes := []*manifestChange{newManifestChange(mcreate, table.id, 0, table.CompressionType())}
	if err := ls.manifest.AddManifestChange(changes); err != nil {
		return err
	}

	// try add the table to the level0
	for !ls.ls[0].Add(table) {
		start := time.Now()
		// level0 is full, wait for while and try again
		for ls.ls[0].NumTables() >= ls.opts.NumLevelZeroTablesStall {
			time.Sleep(10 * time.Millisecond)
		}

		duration := time.Since(start)
		if duration > time.Second {
			ls.opts.Logger.Infof("L0 was stalled for %s ms\n", duration.Round(time.Millisecond))
		}

		atomic.AddInt64(&ls.l0stallMs, int64(duration.Round(time.Millisecond)))
	}

	return nil
}

// Close will close all levels in this levels.
// If there are errors ocurred, then the first will be returned.
func (ls *levels) Close() error {
	var firstErr error
	for _, l := range ls.ls {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return Wrap(firstErr, "Levels.Close")
}

// LevelMeta contains the information about a level.
type LevelMeta struct {
	IsBaseLevel bool
	Id          int
	NumTables   int
	Size        int64
	TargetSize  int64
	MaxFileSize int64
	Score       float64
	Adjusted    float64
}

func (ls *levels) GetLevelMeta() (meta []LevelMeta) {
	dls := ls.getDynamicLevelSize()
	cps := ls.pickCompactionLevels()
	meta = make([]LevelMeta, len(ls.ls))

	for i, level := range ls.ls {
		level.RLock()
		meta[i] = LevelMeta{
			Id:        level.id,
			NumTables: len(level.tables),
			Size:      level.totalSize,
		}
		level.RUnlock()

		meta[i].IsBaseLevel = dls.baseLevel == level.id
		meta[i].TargetSize = dls.targetSize[i]
		meta[i].MaxFileSize = dls.fileSize[i]
	}

	for _, cp := range cps {
		meta[cp.level].Score = cp.score
		meta[cp.level].Adjusted = cp.adjusted
	}

	return
}

// TableMeta contains the information about a table.
type TableMeta struct {
	Id               uint64
	Level            int
	Smallest         []byte
	Biggest          []byte
	KeyCount         uint32
	OnDiskSize       uint32
	UncompressedSize uint32
	IndexSize        uint32
	BloomFilterSize  uint32
	MaxVersion       uint64
}

// GetTableMeta will returns the meta of all tables in the levels.
// The metas are sorted by by level's id and table's id in ascending order
func (ls *levels) GetTableMeta() (meta []TableMeta) {
	for _, level := range ls.ls {
		level.RLock()

		for _, table := range level.tables {
			m := TableMeta{
				Id:               table.ID(),
				Level:            level.id,
				Smallest:         table.Smallest(),
				Biggest:          table.Biggest(),
				KeyCount:         table.KeyCount(),
				OnDiskSize:       table.OnDiskSize(),
				UncompressedSize: table.UncompressedSize(),
				IndexSize:        uint32(table.IndexSize()),
				BloomFilterSize:  uint32(table.BloomFilterSize()),
				MaxVersion:       table.MaxVersion(),
			}

			meta = append(meta, m)
		}

		level.RUnlock()
	}

	// first sort by level in ascending order, then sort by table id in ascending order
	sort.Slice(meta, func(i, j int) bool {
		if meta[i].Level != meta[j].Level {
			return meta[i].Id < meta[j].Id
		}

		return meta[i].Level < meta[j].Level
	})

	return
}

// getDynamicLevelSize calculates the targets for levels in the LSM tree. The idea comes from Dynamic Level
// Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html ) in RocksDB.
func (ls *levels) getDynamicLevelSize() dynamicLevelSize {
	d := dynamicLevelSize{
		targetSize: make([]int64, len(ls.ls)),
		fileSize:   make([]int64, len(ls.ls)),
	}

	dbSize := ls.LastLevel().TotalSize()
	for i := len(ls.ls) - 1; i >= 0; i-- {
		target := dbSize
		if target < ls.opts.BaseLevelSize {
			target = ls.opts.BaseLevelSize
		}

		d.targetSize[i] = target
		if d.baseLevel == 0 && target <= ls.opts.BaseLevelSize {
			d.baseLevel = i
		}

		dbSize /= int64(ls.opts.LevelSizeMultiplier)
	}

	tableSize := ls.opts.BaseTableSize
	for i := 0; i < len(ls.ls); i++ {
		if i == 0 {
			// level0 not include in dynamic size
			d.fileSize[i] = int64(ls.opts.MemTableSize)
		} else if i < d.baseLevel {
			d.fileSize[i] = tableSize
		} else {
			tableSize *= int64(ls.opts.TableSizeMultiplier)
			d.fileSize[i] = tableSize
		}
	}

	// Bring the base level down to the last empty level.
	for i := d.baseLevel + 1; i < len(ls.ls)-1; i++ {
		if ls.ls[i].TotalSize() > 0 {
			break
		}

		d.baseLevel = i
	}

	// If the base level is empty and the next level size is less than the
	// target size, pick the next level as the base level.
	b := d.baseLevel
	lvl := ls.ls
	if b < len(lvl)-1 && lvl[b].TotalSize() == 0 &&
		lvl[b+1].TotalSize() < d.targetSize[b+1] {
		d.baseLevel++
	}

	return d
}

// pickCompactLevels determines which level to compact.
// see https://github.com/facebook/rocksdb/wiki/Choose-Level-Compaction-Files
func (ls *levels) pickCompactionLevels() (cps []compactionPriority) {
	dls := ls.getDynamicLevelSize()
	addPriority := func(level int, score float64) {
		cp := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			dls:      dls,
		}

		cps = append(cps, cp)
	}

	// add level 0 priority based on the number of tables.
	addPriority(0, float64(ls.ls[0].NumTables())/float64(ls.opts.NumLevelZeroTables))

	// All other levels use tables' size to calculate priority.
	for i := 1; i < len(ls.ls); i++ {
		// Don't consider those tables that are already being compacted right now.
		delSize := ls.cstatus.DeleteSize(i)

		l := ls.ls[i]
		sz := l.TotalSize() - delSize
		addPriority(i, float64(sz)/float64(dls.targetSize[i]))
	}

	// If Ln score is >= 1.0, that means this level is already overflowing, otherwise not full.
	// If the bottom level is already overflowing, then de-prioritize compaction of the above level.
	// If the bottom level is not full, then increase the priority of above level.
	var prevLevel int
	for level := dls.baseLevel; level < len(ls.ls); level++ {
		if cps[prevLevel].adjusted >= 1 {
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			const minScore = 0.01
			if cps[level].score >= minScore {
				cps[prevLevel].adjusted /= cps[level].adjusted
			} else {
				cps[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// Pick all the levels whose original score is >= 1.0, that need do compaction.
	// If there are n levels, then need to do n-1 compaction at most. So only need the n-1 levels
	out := []compactionPriority{}
	for _, cp := range cps[:len(cps)-1] {
		if cp.score >= 1.0 {
			out = append(out, cp)
		}
	}

	cps = out
	// sort by adjusted score in descending order.
	// The higher the adjusted score, the higher the priority of compaction
	sort.Slice(cps, func(i, j int) bool {
		return cps[i].adjusted > cps[j].adjusted
	})

	return
}

func (ls *levels) isOverlapsWith(tables []*Table, fromLevel int) bool {
	krange := getKeyRange(tables...)
	for _, level := range ls.ls {
		if level.id < fromLevel {
			continue
		}

		level.RLock()
		left, right := level.OverlappingTables(krange.left, krange.right)
		level.RUnlock()

		if right > left {
			return true
		}
	}

	return false
}

func (ls *levels) StartCompact(closer *internel.Closer) {
	n := ls.opts.NumCompactors
	closer.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		go ls.runCompact(i, closer)
	}
}

func (ls *levels) runCompact(compactId int, closer *internel.Closer) {
	defer closer.Done()

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-closer.HasBeenClosed():
		randomDelay.Stop()
		return
	}

	// if there is a L0 compact, run it first always.
	moveL0toFront := func(cps []compactionPriority) []compactionPriority {
		idx := -1
		for i, p := range cps {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]compactionPriority{}, cps[idx])
			out = append(out, cps[:idx]...)
			out = append(out, cps[idx+1:]...)
			return out
		}

		return cps
	}

	run := func(p compactionPriority) bool {
		err := ls.doCompact(compactId, p)
		if err == nil {
			return true
		}

		if err != ErrFillTable {
			ls.opts.Logger.Warningf("While running doCompact: %v\n", err)
		}
		return false
	}

	runOnce := func() bool {
		cps := ls.pickCompactionLevels()
		if compactId == 0 {
			// Worker ID zero prefers to compact L0 always.
			cps = moveL0toFront(cps)
		}

		for _, p := range cps {
			if compactId == 0 && p.level == 0 || p.adjusted >= 1.0 {
				if run(p) {
					return true
				}
			} else {
				break
			}
		}

		return false
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			runOnce()
		case <-closer.HasBeenClosed():
			return
		}
	}
}

func (ls *levels) doCompact(compactId int, cp compactionPriority) error {
	lid := cp.level
	if cp.dls.baseLevel == 0 {
		cp.dls = ls.getDynamicLevelSize()
	}

	ci := compactInfo{
		compactId:    compactId,
		cp:           cp,
		curr:         ls.ls[lid],
		dropPrefixes: cp.dropPrefixes,
	}

	if lid == 0 {
		ci.next = ls.ls[cp.dls.baseLevel]
		if !ls.fillTablesL0(&ci) {
			return ErrFillTable
		}
	} else {
		ci.next = ci.curr
		// We're not compacting the last level so pick the next level.
		if !ci.curr.IsLastLevel() {
			ci.next = ls.ls[lid+1]
		}

		if !ls.fillTables(&ci) {
			return ErrFillTable
		}
	}

	defer ls.cstatus.Delete(ci)

	if err := ls.compact(compactId, lid, ci); err != nil {
		ls.opts.Logger.Warningf("[Compact: %d] LOG Compact FAILED with error: %+v: %+v",
			compactId, err, ci)
		return err
	}

	ls.opts.Logger.Debugf("[Compact: %d] compaction for level: %d DONE.", compactId, ci.curr.id)
	return nil
}

// compact picks some table on level l and compacts it away to the next level.
func (ls *levels) compact(compactId, levelId int, ci compactInfo) (err error) {
	if len(ci.cp.dls.fileSize) == 0 {
		return errors.New("filesizes cannot be zero. Dynamicfilesize are not set")
	}

	start := time.Now()

	curr := ci.curr
	next := ci.next

	// not compact L0 to L0
	if curr.id != next.id {
		ls.splitSubCompact(&ci)
	}

	if len(ci.splits) == 0 {
		ci.splits = append(ci.splits, keyRange{})
	}

	newTables, decr, err := ls.buildNewTables(levelId, ci)
	if err != nil {
		return err
	}

	defer func() {
		// decrease ref to the old tables (delete the old table file)
		if derr := decr(); derr != nil {
			err = derr
		}
	}()

	// apply changes to manifest file
	changes := getCompactChanges(&ci, newTables)
	if err := ls.manifest.AddManifestChange(changes); err != nil {
		return err
	}

	// replace toTables with the newTables first and then delete tables from fromTables
	if err := next.Replace(ci.toTables, newTables); err != nil {
		return err
	}
	if err := curr.Delete(ci.fromTables); err != nil {
		return err
	}

	// If the time of compact longer than 2s, logging it
	if duration := time.Since(start); duration > 2*time.Second {
		ls.opts.Logger.Infof("[%d] LOG Compact level(%d)->level(%d) (%d, %d -> %d tables with %d splits)"+
			"[tables' id %s/%s -> %s] took %d Milliseconds.\n",
			compactId, curr.id, next.id, len(ci.fromTables), len(ci.toTables),
			len(newTables), len(ci.splits), strings.Join(getTablesId(ci.fromTables), " "),
			strings.Join(getTablesId(ci.toTables), " "),
			strings.Join(getTablesId(newTables), " "),
			duration.Round(time.Millisecond))
	}

	return nil
}

func getTablesId(tables []*Table) []string {
	var res []string
	for _, table := range tables {
		res = append(res, fmt.Sprintf("%05d", table.ID()))
	}

	return res
}

func (ls *levels) buildNewTables(levelId int, ci compactInfo) ([]*Table, func() error, error) {
	from, to := ci.fromTables, ci.toTables

	var valid []*Table
	for _, table := range to {
		if len(ci.dropPrefixes) > 0 {
			for _, prefix := range ci.dropPrefixes {
				if bytes.HasPrefix(table.Smallest(), prefix) &&
					bytes.HasPrefix(table.Biggest(), prefix) {
					continue
				}

				valid = append(valid, table)
			}
		} else {
			valid = append(valid, table)
		}
	}

	newIterators := func() []Iterator {
		var iters []Iterator
		if levelId == 0 {
			// level0 should iterate reversed (from newest to oldest)
			for i := len(from) - 1; i >= 0; i-- {
				iters = append(iters, from[i].NewIterator(false))
			}
		} else {
			iters = append(iters, from[0].NewIterator(false))
		}

		if valid != nil {
			iters = append(iters, NewTablesIterator(valid, false))
		}
		return iters
	}

	res := make(chan *Table, 3)
	// CAUTION: Even in a subcompact goroutine, multiple tables will be built at the same time
	// by some goroutines (there is 8 at most).
	builders := internel.NewThrottle(8 + int(maxSubCompact))
	for _, kr := range ci.splits {
		if err := builders.Do(); err != nil {
			ls.opts.Logger.Errorf("can't start subcompaction: %+v", err)
			return nil, nil, err
		}

		go func(kr keyRange) {
			defer builders.Done(nil)
			it := NewTablesMergeIterator(ls.opts, newIterators(), false)
			defer it.Close()
			ls.subcompact(it, kr, ci, builders, res)
		}(kr)
	}

	var newTables []*Table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// wait for all subcompact finished, and all newTables picked up
	err := builders.Finish()
	close(res)
	wg.Wait()

	if err == nil {
		// sync dir, because new file created.
		err = syncDir(ls.opts.Dir)
	}

	// there is an error happened. So Delete all the newly created table files
	// (by calling DecrRef because new table only has one ref).
	if err != nil {
		_ = decrRefs(newTables)
		return nil, nil, Wrapf(err, "while running compaction: %+v", ci)
	}

	// sort newly created tables by key in ascending order
	sort.Slice(newTables, func(i, j int) bool {
		return ls.opts.comparator(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})

	return newTables, func() error { return decrRefs(newTables) }, nil
}

const maxSubCompact = 5.0

// splitSubCompact can allow us to run multiple sub-compactions in parallel
// across the split key ranges.
func (ls *levels) splitSubCompact(ci *compactInfo) {
	// clear
	ci.splits = ci.splits[:0]

	width := int(math.Ceil(float64(len(ci.toTables)) / maxSubCompact))
	if width < 3 {
		// at least 3
		width = 3
	}

	srange := ci.currRange
	srange.MergeFrom(ci.nextRange)

	appendRange := func(right []byte) {
		srange.right = make([]byte, len(right))
		copy(srange.right, right)

		ci.splits = append(ci.splits, srange)
		srange.left = srange.right
	}

	for i, table := range ci.toTables {
		if i == len(ci.toTables)-1 {
			// last table
			appendRange([]byte{})
			break
		}

		if (i+1)%width == 0 {
			right := KeyWithVersion(ParseKey(table.Biggest()), 0)
			appendRange(right)
		}
	}
}

func (ls *levels) subcompact(it Iterator, kr keyRange, ci compactInfo,
	builderThrottle *internel.Throttle, res chan<- *Table) {
	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	hasOverlap := ls.isOverlapsWith(ci.tables(), ci.next.id+1)

	// Pick a discard version, so we can discard versions below this version.
	discardVersion := ls.MaxVersion()
	if !ls.snapshots.Empty() {
		discardVersion = ls.snapshots.Oldest().version
	}

	// Try to collect stats so that we can inform value log about GC.
	// That would help us find which value log file should be GCed.
	discardStats := make(map[uint32]uint64)
	updateStats := func(vs EValue) {
		if vs.Meta&ValPtr > 0 {
			var vp valPtr
			vp.Decode(vs.Value)
			discardStats[vp.fid] += uint64(vp.len)
		}
	}

	// exceedsAllowedOverlap returns true if the given key range would overlap with more than 10
	// tables from level below nextLevel (nextLevel+1). This helps avoid generating tables at Li
	// with huge overlaps with Li+1, that will slow down the compact for Li
	exceedsAllowedOverlap := func(kr keyRange) bool {
		next := ci.next.id + 1
		if next <= 1 || next >= len(ls.ls) {
			return false
		}

		nextLevel := ls.ls[next]
		nextLevel.RLock()
		defer nextLevel.RUnlock()

		l, r := nextLevel.OverlappingTables(kr.left, kr.right)
		return r-l >= 10
	}

	var (
		lastKey, skipKey []byte
		numBuilds        int
	)

	buildTable := func(builder *TableBuilder) {
		start := time.Now()
		var numKeys, numSkips uint64
		var rangeCheck int
		var krange keyRange

		for ; it.Valid(); it.Next() {
			curr := it.Key()
			// check if need to skip the prefix
			if len(ci.dropPrefixes) > 0 {
				for _, prefix := range ci.dropPrefixes {
					if bytes.HasPrefix(curr, prefix) {
						numSkips++
						updateStats(it.Value())
						break
					}
				}
			}

			if len(skipKey) > 0 {
				if SameKey(curr, skipKey) {
					numSkips++
					updateStats(it.Value())
					continue
				}

				skipKey = skipKey[:0]
			}

			if !SameKey(curr, lastKey) {
				if kr.right != nil && len(kr.right) > 0 && ls.opts.comparator(curr, kr.right) >= 0 {
					// no more record in the keyRange that need to compact
					break
				}

				if builder.ReachedCapacity() {
					// Only break if we are on a different key, and have reached capacity.
					// Ensure that all versions of the key are stored in the same table file,
					// and not divided across multiple tables at the same level.
					break
				}

				lastKey = make([]byte, len(curr))
				copy(lastKey, curr)

				if len(krange.left) == 0 {
					krange.left = make([]byte, len(curr))
					copy(krange.left, curr)
				}

				krange.right = lastKey

				rangeCheck++
				if rangeCheck%5000 == 0 {
					// This table's range exceeds the allowed range overlap with the level after
					// next. So, stop writing to this table.
					if exceedsAllowedOverlap(krange) {
						ls.opts.Logger.Debugf("L%d -> L%d Breaking due to exceedsAllowedOverlap with kr: %s\n",
							ci.curr.id, ci.next.id, krange)
						break
					}
				}
			}

			numKeys++
			v := it.Value()
			version := ParseVersion(curr)
			isDeleted := v.Meta&Delete > 0
			// the version of key below the discardVersion, the key should be discarded.
			if version < discardVersion {
				// just keep the last version of the key, deleted all the rest of the versions.
				skipKey = make([]byte, len(curr))
				copy(skipKey, curr)

				// the key deleted and not overlap with lower levels
				// so the value in the vlog doesn't need to keep
				if !hasOverlap && isDeleted {
					numSkips++
					updateStats(v)
					continue
				}
			}

			var vp valPtr
			if v.Meta&ValPtr > 0 {
				vp.Decode(v.Value)
			}

			builder.Add(curr, v, vp.len)
		}

		ls.opts.Logger.Debugf("[%d] LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			ci.compactId, numKeys, numSkips, time.Since(start).Round(time.Millisecond))
	}

	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		it.SeekToFirst()
	}

	for it.Valid() {
		// the tables has no any key in inputed keyRange
		if len(kr.right) > 0 && ls.opts.comparator(it.Key(), kr.right) >= 0 {
			break
		}

		newOpts := *ls.opts
		newOpts.tableSize = uint64(ci.cp.dls.fileSize[ci.next.id])
		builder := NewTableBuilder(newOpts)
		buildTable(builder)

		if builder.Empty() {
			builder.Finish()
			continue
		}

		numBuilds++
		tableId := ls.nextTableId()
		if err := builderThrottle.Do(); err != nil {
			break
		}

		go func(builder *TableBuilder) {
			var err error
			defer builderThrottle.Done(err)

			fname := NewTableFileName(tableId, ls.opts.Dir)
			table, err := CreateTable(fname, builder)
			if err != nil {
				return
			}

			res <- table
		}(builder)
	}

	ls.vlog.updateDiscard(discardStats)
	ls.opts.Logger.Debugf("Discard stats: %+v", discardStats)
}

func (ls *levels) nextTableId() uint64 {
	id := atomic.AddUint64(&ls.nextId, 1)

	return id - 1
}

// fillTablesL0 would try to fill tables from L0 to be compacted with Lbase. If
// it can not do that, it would try to compact tables from L0 -> L0.
func (ls *levels) fillTablesL0(ci *compactInfo) bool {
	if ls.fillTablesL0ToBase(ci) {
		return true
	}
	return false
	//return ls.fillTablesL0ToL0(ci)
}

func (ls *levels) fillTablesL0ToBase(ci *compactInfo) bool {
	if ci.next.id == 0 {
		return false
	}

	if ci.cp.adjusted > 0.0 && ci.cp.adjusted < 1.0 {
		return false
	}

	ci.lock()
	defer ci.unlock()

	// fill fromTables
	currTables := ci.curr.tables
	if len(currTables) == 0 {
		return false
	}

	var fromTables []*Table
	if len(ci.dropPrefixes) > 0 {
		fromTables = currTables
	} else {
		var currRange keyRange
		// start from the oldest file first. Find those tables that its key range overlap
		for _, table := range currTables {
			kr := getKeyRange(table)
			if currRange.IsOverlapsWith(kr) {
				fromTables = append(fromTables, table)
				currRange.MergeFrom(kr)
			} else {
				break
			}
		}
	}

	// fill currRange
	ci.currRange = getKeyRange(fromTables...)
	ci.fromTables = fromTables

	// fill toTables
	left, right := ci.next.OverlappingTables(ci.currRange.left, ci.currRange.right)
	ci.toTables = make([]*Table, right-left)
	copy(ci.toTables, ci.next.tables[left:right])

	// fill nextRange
	if len(ci.toTables) == 0 {
		ci.nextRange = ci.currRange
	} else {
		ci.nextRange = getKeyRange(ci.toTables...)
	}

	// check and update compact status
	return ls.cstatus.Check(*ci)
}

// fillTablesL0ToL0 can only run by 0 compactor(id=0).
// When it running, not allow any other L0 to Base compators run
func (ls *levels) fillTablesL0ToL0(ci *compactInfo) bool {
	if ci.compactId != 0 {
		return false
	}

	// trivial fill
	ci.next = ls.ls[0]
	ci.nextRange = keyRange{}
	ci.toTables = nil

	ci.lock()
	defer ci.unlock()

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	ls.cstatus.Lock()
	defer ls.cstatus.Unlock()

	// fill fromTables
	currTables := ci.curr.tables
	var fromTables []*Table
	start := time.Now()
	for _, table := range currTables {
		if table.Size() >= 2*ci.cp.dls.fileSize[0] {
			// this table's size is already enough, not need compact any more.
			continue
		}

		if _, running := ls.cstatus.tables[table.ID()]; running {
			// there is already a running compaction included this table
			continue
		}

		if start.Sub(table.CreatedAt) < 10*time.Second {
			// just run a compaction 10 second ago, don't need to compact it again.
			continue
		}

		fromTables = append(fromTables, table)
	}

	// at least 5 tables, don't do compact with a few tables
	if len(fromTables) < 4 {
		return false
	}

	ci.fromTables = fromTables
	// fill currRange
	ci.currRange = infKeyRange

	currLevelStatus := ls.cstatus.levels[ci.curr.id]
	currLevelStatus.ranges = append(currLevelStatus.ranges, infKeyRange)
	for _, t := range fromTables {
		ls.cstatus.tables[t.ID()] = struct{}{}
	}

	// set the target file size to max, so the output is always one file.
	ci.cp.dls.fileSize[0] = math.MaxUint32
	return true
}

func (ls *levels) fillTables(ci *compactInfo) bool {
	ci.lock()
	defer ci.unlock()

	if ci.curr.IsLastLevel() {
		return false
	}

	if len(ci.curr.tables) == 0 {
		return false
	}

	tables := make([]*Table, len(ci.curr.tables))
	copy(tables, ci.curr.tables)

	// make the oldest table in the first.
	// This is similar to kOldestLargestSeqFirst in RocksDB.
	sortByMaxVersion(tables)

	for _, table := range tables {
		ci.size = table.Size()
		ci.currRange = getKeyRange(table)
		if ls.cstatus.IsOverlapsWith(ci.curr.id, ci.currRange) {
			// this table is already compacting, don't compact it again
			continue
		}

		ci.fromTables = []*Table{table}

		// fill toTables
		left, right := ci.next.OverlappingTables(ci.currRange.left, ci.currRange.right)
		if left == right {
			// not found any next level tables overlapped
			ci.toTables = []*Table{}
			ci.nextRange = ci.currRange
			if !ls.cstatus.Check(*ci) {
				continue
			}

			return true
		}
		ci.toTables = make([]*Table, right-left)
		copy(ci.toTables, ci.next.tables[left:right])

		// fill nextRange
		ci.nextRange = getKeyRange(ci.toTables...)
		if ls.cstatus.IsOverlapsWith(ci.next.id, ci.nextRange) {
			// there is a running compactor with this table
			continue
		}

		if !ls.cstatus.Check(*ci) {
			// there is a running compactor with this table
			continue
		}

		return true
	}

	return false
}

func (ls *levels) appendIterators(iters []Iterator, option *ReadOptions) []Iterator {
	for _, level := range ls.ls {
		iters = level.appendIterators(iters, option)
	}

	return iters
}

func (ls *levels) dropTree() (int, error) {
	var all []*Table
	for _, l := range ls.ls {
		l.RLock()
		all = append(all, l.tables...)
		l.RUnlock()
	}

	if len(all) == 0 {
		return 0, nil
	}

	changes := manifestChanges{}
	for _, table := range all {
		changes = append(changes, newManifestChange(mdelete, table.id, 0, table.CompressionType()))
	}

	if err := ls.manifest.AddManifestChange(changes); err != nil {
		return 0, err
	}

	for _, l := range ls.ls {
		l.Lock()
		l.totalSize = 0
		l.tables = l.tables[:0]
		l.Unlock()
	}

	for _, table := range all {
		if err := table.DecrRef(); err != nil {
			return 0, err
		}
	}

	return len(all), nil
}

func sortByMaxVersion(tables []*Table) {
	if len(tables) == 0 {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].MaxVersion() < tables[j].MaxVersion()
	})
}

func loadTables(opts *Options, mf *Manifest) ([][]*Table, uint64, error) {
	var numTables int32
	var maxFileID uint64

	start := time.Now()
	// ticker can make smooth growth of disk read workload
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// throttle can limit the number of goroutines for load tables
	throttle := internel.NewThrottle(3)

	var tm sync.Mutex
	tables := make([][]*Table, opts.MaxLevels)
	for id, tmf := range mf.Tables {
		tableFileName := NewTableFileName(id, opts.Dir)

		select {
		case <-ticker.C:
			opts.Logger.Infof("%d tables out of %d opened in %s\n", atomic.LoadInt32(&numTables),
				len(mf.Tables), time.Since(start).Round(time.Millisecond))
		default:
		}

		if err := throttle.Do(); err != nil {
			closeAllTables(tables)
			return nil, 0, err
		}

		if id > maxFileID {
			maxFileID = id
		}

		go func(fname string, tf tableManifest) {
			var rerr error
			defer func() {
				throttle.Done(rerr)
				atomic.AddInt32(&numTables, 1)
			}()

			tableOpts := *opts
			tableOpts.CompressionType = tf.compression

			mf, err := internel.OpenMmapFile(fname, os.O_RDWR, 0)
			if err != nil {
				rerr = Wrapf(err, "Opening file: %q", fname)
				return
			}
			t, err := OpenTable(mf, tableOpts)
			if err != nil {
				rerr = Wrapf(err, "Opening table: %q", fname)
				return
			}

			tm.Lock()
			tables[tf.level] = append(tables[tf.level], t)
			tm.Unlock()
		}(tableFileName, tmf)
	}

	if err := throttle.Finish(); err != nil {
		closeAllTables(tables)
		return nil, 0, err
	}

	opts.Logger.Infof("All %d tables opened in %s\n", atomic.LoadInt32(&numTables),
		time.Since(start).Round(time.Millisecond))
	return tables, maxFileID, nil
}

func closeAllTables(tables [][]*Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close()
		}
	}
}

func checkTablesByManifest(opts *Options, mf *Manifest) error {
	// 0. get tables from dir scan
	tableMap := getTableIdMap(opts.Dir)

	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := tableMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range tableMap {
		if _, ok := mf.Tables[id]; !ok {
			opts.Logger.Debugf("Table file %d not referenced in MANIFEST\n", id)
			filename := NewTableFileName(id, opts.Dir)
			if err := os.Remove(filename); err != nil {
				return Wrapf(err, "While removing table %d", id)
			}
		}
	}

	return nil
}

func getTableIdMap(dir string) map[uint64]struct{} {
	files, err := ioutil.ReadDir(dir)
	Check(err)

	idMap := map[uint64]struct{}{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileId, ok := GetFileID(file.Name())
		if !ok {
			continue
		}

		idMap[fileId] = struct{}{}
	}

	return idMap
}

func getCompactChanges(ci *compactInfo, newTables []*Table) manifestChanges {
	changes := manifestChanges{}
	// create newTables changes
	for _, table := range newTables {
		changes = append(changes,
			newManifestChange(mcreate, table.ID(), uint64(ci.next.id), table.CompressionType()))
	}

	// create fromTables delete changes
	for _, table := range ci.fromTables {
		changes = append(changes,
			newManifestChange(mdelete, table.ID(), uint64(ci.curr.id), table.CompressionType()))
	}

	// create toTables delete changes
	for _, table := range ci.toTables {
		changes = append(changes,
			newManifestChange(mdelete, table.ID(), uint64(ci.next.id), table.CompressionType()))
	}

	return changes
}

func containsPrefix(table *Table, prefix []byte) bool {
	smallest := table.Smallest()
	biggest := table.Biggest()

	if bytes.HasPrefix(smallest, prefix) {
		return true
	}

	if bytes.HasPrefix(biggest, prefix) {
		return true
	}

	// prefix may be in range [smallest, biggest]
	if bytes.Compare(smallest, prefix) < 0 ||
		bytes.Compare(biggest, prefix) > 0 {
		iter := table.NewIterator(false)
		defer iter.Close()

		iter.Seek(KeyWithVersion(prefix, math.MaxUint64))
		if iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
			return true
		}
	}

	return false
}

func containsAnyPrefixes(table *Table, prefixes [][]byte) bool {
	for _, prefix := range prefixes {
		if containsPrefix(table, prefix) {
			return true
		}
	}

	return false
}
