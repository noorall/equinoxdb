package compactor

// Compactions are the process of creating read-optimized TSM files.
// The files are created by converting write-optimized WAL entries
// to read-optimized TSM format.  They can also be created from existing
// TSM files when there are tombstone records that need to be removed, points
// that were overwritten by later writes and need to updated, or multiple
// smaller TSM files need to be merged to reduce file counts and improve
// compression ratios.
//
// The compaction process is stream-oriented using multiple readers and
// iterators.  The resulting stream is written sorted and chunked to allow for
// one-pass writing of a new TSM file.

import (
	"bytes"
	"equinox/pkg/limiter"
	"equinox/storage/memory"
	"equinox/storage/store"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const DefaultSegmentSize = 10 * 1024 * 1024
const maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB
const logEvery = 2 * DefaultSegmentSize
const DefaultMaxPointsPerBlock = 1000

const (
	// DefaultMaxSavedErrors is the number of errors that are stored by a TSMBatchKeyReader before
	// subsequent errors are discarded
	DefaultMaxSavedErrors = 100
)

var (
	errMaxFileExceeded     = fmt.Errorf("max file exceeded")
	errSnapshotsDisabled   = fmt.Errorf("snapshots disabled")
	errCompactionsDisabled = fmt.Errorf("compactions disabled")
)

type errCompactionInProgress struct {
	err error
}

// Error returns the string representation of the error, to satisfy the error interface.
func (e errCompactionInProgress) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction in progress: %s", e.err)
	}
	return "compaction in progress"
}

type errCompactionAborted struct {
	err error
}

func (e errCompactionAborted) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction aborted: %s", e.err)
	}
	return "compaction aborted"
}

type errBlockRead struct {
	file string
	err  error
}

func (e errBlockRead) Error() string {
	if e.err != nil {
		return fmt.Sprintf("block read error on %s: %s", e.file, e.err)
	}
	return fmt.Sprintf("block read error on %s", e.file)
}

// CompactionGroup represents a list of files eligible to be compacted together.
type CompactionGroup []string

// CompactionPlanner determines what TSM files and WAL segments to include in a
// given compaction run.
type CompactionPlanner interface {
	Plan(lastWrite time.Time) ([]CompactionGroup, int64)
	PlanLevel(level int) ([]CompactionGroup, int64)
	PlanOptimize() ([]CompactionGroup, int64)
	Release(group []CompactionGroup)
	FullyCompacted() (bool, string)

	// ForceFull causes the planner to return a full compaction plan the next
	// time Plan() is called if there are files that could be compacted.
	ForceFull()

	SetFileStore(fs *store.FileStore)
}

// DefaultPlanner implements CompactionPlanner using a strategy to roll up
// multiple generations of TSM files into larger files in stages.  It attempts
// to minimize the number of TSM files on disk while rolling up a bounder number
// of files.
type DefaultPlanner struct {
	FileStore fileStore

	// compactFullWriteColdDuration specifies the length of time after
	// which if no writes have been committed to the WAL, the engine will
	// do a full compaction of the TSM files in this shard. This duration
	// should always be greater than the CacheFlushWriteColdDuration
	compactFullWriteColdDuration time.Duration

	// lastPlanCheck is the last time Plan was called
	lastPlanCheck time.Time

	mu sync.RWMutex
	// lastFindGenerations is the last time findGenerations was run
	lastFindGenerations time.Time

	// lastGenerations is the last set of generations found by findGenerations
	lastGenerations tsmGenerations

	// forceFull causes the next full plan requests to plan any files
	// that may need to be compacted.  Normally, these files are skipped and scheduled
	// infrequently as the plans are more expensive to run.
	forceFull bool

	// filesInUse is the set of files that have been returned as part of a plan and might
	// be being compacted.  Two plans should not return the same file at any given time.
	filesInUse map[string]struct{}
}

type fileStore interface {
	Stats() []store.FileStat
	LastModified() time.Time
	BlockCount(path string, idx int) int
	ParseFileName(path string) (int, int, error)
}

func NewDefaultPlanner(fs fileStore, writeColdDuration time.Duration) *DefaultPlanner {
	return &DefaultPlanner{
		FileStore:                    fs,
		compactFullWriteColdDuration: writeColdDuration,
		filesInUse:                   make(map[string]struct{}),
	}
}

// tsmGeneration represents the TSM files within a generation.
// 000001-01.tsm, 000001-02.tsm would be in the same generation
// 000001 each with different sequence numbers.
type tsmGeneration struct {
	id            int
	files         []store.FileStat
	parseFileName store.ParseFileNameFunc
}

func newTsmGeneration(id int, parseFileNameFunc store.ParseFileNameFunc) *tsmGeneration {
	return &tsmGeneration{
		id:            id,
		parseFileName: parseFileNameFunc,
	}
}

// size returns the total size of the files in the generation.
func (t *tsmGeneration) size() uint64 {
	var n uint64
	for _, f := range t.files {
		n += uint64(f.Size)
	}
	return n
}

// compactionLevel returns the level of the files in this generation.
func (t *tsmGeneration) level() int {
	// Level 0 is always created from the result of a cache compaction.  It generates
	// 1 file with a sequence num of 1.  Level 2 is generated by compacting multiple
	// level 1 files.  Level 3 is generate by compacting multiple level 2 files.  Level
	// 4 is for anything else.
	_, seq, _ := t.parseFileName(t.files[0].Path)
	if seq < 4 {
		return seq
	}

	return 4
}

// count returns the number of files in the generation.
func (t *tsmGeneration) count() int {
	return len(t.files)
}

// hasTombstones returns true if there are keys removed for any of the files.
func (t *tsmGeneration) hasTombstones() bool {
	for _, f := range t.files {
		if f.HasTombstone {
			return true
		}
	}
	return false
}

func (c *DefaultPlanner) SetFileStore(fs *store.FileStore) {
	c.FileStore = fs
}

func (c *DefaultPlanner) ParseFileName(path string) (int, int, error) {
	return c.FileStore.ParseFileName(path)
}

// FullyCompacted returns true if the shard is fully compacted.
func (c *DefaultPlanner) FullyCompacted() (bool, string) {
	gens := c.findGenerations(false)
	if len(gens) > 1 {
		return false, "not fully compacted and not idle because of more than one generation"
	} else if gens.hasTombstones() {
		return false, "not fully compacted and not idle because of tombstones"
	} else {
		return true, ""
	}
}

// ForceFull causes the planner to return a full compaction plan the next time
// a plan is requested.  When ForceFull is called, level and optimize plans will
// not return plans until a full plan is requested and released.
func (c *DefaultPlanner) ForceFull() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forceFull = true
}

// PlanLevel returns a set of TSM files to rewrite for a specific level.
func (c *DefaultPlanner) PlanLevel(level int) ([]CompactionGroup, int64) {
	// If a full plan has been requested, don't plan any levels which will prevent
	// the full plan from acquiring them.
	c.mu.RLock()
	if c.forceFull {
		c.mu.RUnlock()
		return nil, 0
	}
	c.mu.RUnlock()

	// Determine the generations from all files on disk.  We need to treat
	// a generation conceptually as a single file even though it may be
	// split across several files in sequence.
	generations := c.findGenerations(true)

	// If there is only one generation and no tombstones, then there's nothing to
	// do.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Group each generation by level such that two adjacent generations in the same
	// level become part of the same group.
	var currentGen tsmGenerations
	var groups []tsmGenerations
	for i := 0; i < len(generations); i++ {
		cur := generations[i]

		// See if this generation is orphaned which would prevent it from being further
		// compacted until a final full compaction runs.
		if i < len(generations)-1 {
			if cur.level() < generations[i+1].level() {
				currentGen = append(currentGen, cur)
				continue
			}
		}

		if len(currentGen) == 0 || currentGen.level() == cur.level() {
			currentGen = append(currentGen, cur)
			continue
		}
		groups = append(groups, currentGen)

		currentGen = tsmGenerations{}
		currentGen = append(currentGen, cur)
	}

	if len(currentGen) > 0 {
		groups = append(groups, currentGen)
	}

	// Remove any groups in the wrong level
	var levelGroups []tsmGenerations
	for _, cur := range groups {
		if cur.level() == level {
			levelGroups = append(levelGroups, cur)
		}
	}

	minGenerations := 4
	if level == 1 {
		minGenerations = 8
	}

	var cGroups []CompactionGroup
	for _, group := range levelGroups {
		for _, chunk := range group.chunk(minGenerations) {
			var cGroup CompactionGroup
			var hasTombstones bool
			for _, gen := range chunk {
				if gen.hasTombstones() {
					hasTombstones = true
				}
				for _, file := range gen.files {
					cGroup = append(cGroup, file.Path)
				}
			}

			if len(chunk) < minGenerations && !hasTombstones {
				continue
			}

			cGroups = append(cGroups, cGroup)
		}
	}

	if !c.acquire(cGroups) {
		return nil, int64(len(cGroups))
	}

	return cGroups, int64(len(cGroups))
}

// PlanOptimize returns all TSM files if they are in different generations in order
// to optimize the index across TSM files.  Each returned compaction group can be
// compacted concurrently.
func (c *DefaultPlanner) PlanOptimize() ([]CompactionGroup, int64) {
	// If a full plan has been requested, don't plan any levels which will prevent
	// the full plan from acquiring them.
	c.mu.RLock()
	if c.forceFull {
		c.mu.RUnlock()
		return nil, 0
	}
	c.mu.RUnlock()

	// Determine the generations from all files on disk.  We need to treat
	// a generation conceptually as a single file even though it may be
	// split across several files in sequence.
	generations := c.findGenerations(true)

	// If there is only one generation and no tombstones, then there's nothing to
	// do.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Group each generation by level such that two adjacent generations in the same
	// level become part of the same group.
	var currentGen tsmGenerations
	var groups []tsmGenerations
	for i := 0; i < len(generations); i++ {
		cur := generations[i]

		// Skip the file if it's over the max size and contains a full block and it does not have any tombstones
		if cur.count() > 2 && cur.size() > uint64(maxTSMFileSize) && c.FileStore.BlockCount(cur.files[0].Path, 1) == DefaultMaxPointsPerBlock && !cur.hasTombstones() {
			continue
		}

		// See if this generation is orphan'd which would prevent it from being further
		// compacted until a final full compaction runs.
		if i < len(generations)-1 {
			if cur.level() < generations[i+1].level() {
				currentGen = append(currentGen, cur)
				continue
			}
		}

		if len(currentGen) == 0 || currentGen.level() == cur.level() {
			currentGen = append(currentGen, cur)
			continue
		}
		groups = append(groups, currentGen)

		currentGen = tsmGenerations{}
		currentGen = append(currentGen, cur)
	}

	if len(currentGen) > 0 {
		groups = append(groups, currentGen)
	}

	// Only optimize level 4 files since using lower-levels will collide
	// with the level planners
	var levelGroups []tsmGenerations
	for _, cur := range groups {
		if cur.level() == 4 {
			levelGroups = append(levelGroups, cur)
		}
	}

	var cGroups []CompactionGroup
	for _, group := range levelGroups {
		// Skip the group if it's not worthwhile to optimize it
		if len(group) < 4 && !group.hasTombstones() {
			continue
		}

		var cGroup CompactionGroup
		for _, gen := range group {
			for _, file := range gen.files {
				cGroup = append(cGroup, file.Path)
			}
		}

		cGroups = append(cGroups, cGroup)
	}

	if !c.acquire(cGroups) {
		return nil, int64(len(cGroups))
	}

	return cGroups, int64(len(cGroups))
}

// Plan returns a set of TSM files to rewrite for level 4 or higher.  The planning returns
// multiple groups if possible to allow compactions to run concurrently.
func (c *DefaultPlanner) Plan(lastWrite time.Time) ([]CompactionGroup, int64) {
	generations := c.findGenerations(true)

	c.mu.RLock()
	forceFull := c.forceFull
	c.mu.RUnlock()

	// first check if we should be doing a full compaction because nothing has been written in a long time
	if forceFull || c.compactFullWriteColdDuration > 0 && time.Since(lastWrite) > c.compactFullWriteColdDuration && len(generations) > 1 {

		// Reset the full schedule if we planned because of it.
		if forceFull {
			c.mu.Lock()
			c.forceFull = false
			c.mu.Unlock()
		}

		var tsmFiles []string
		var genCount int
		for i, group := range generations {
			var skip bool

			// Skip the file if it's over the max size and contains a full block and it does not have any tombstones
			if len(generations) > 2 && group.size() > uint64(maxTSMFileSize) && c.FileStore.BlockCount(group.files[0].Path, 1) == DefaultMaxPointsPerBlock && !group.hasTombstones() {
				skip = true
			}

			// We need to look at the level of the next file because it may need to be combined with this generation
			// but won't get picked up on it's own if this generation is skipped.  This allows the most recently
			// created files to get picked up by the full compaction planner and avoids having a few less optimally
			// compressed files.
			if i < len(generations)-1 {
				if generations[i+1].level() <= 3 {
					skip = false
				}
			}

			if skip {
				continue
			}

			for _, f := range group.files {
				tsmFiles = append(tsmFiles, f.Path)
			}
			genCount += 1
		}
		sort.Strings(tsmFiles)

		// Make sure we have more than 1 file and more than 1 generation
		if len(tsmFiles) <= 1 || genCount <= 1 {
			return nil, 0
		}

		group := []CompactionGroup{tsmFiles}
		if !c.acquire(group) {
			return nil, int64(len(group))
		}
		return group, int64(len(group))
	}

	// don't plan if nothing has changed in the filestore
	if c.lastPlanCheck.After(c.FileStore.LastModified()) && !generations.hasTombstones() {
		return nil, 0
	}

	c.lastPlanCheck = time.Now()

	// If there is only one generation, return early to avoid re-compacting the same file
	// over and over again.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Need to find the ending point for level 4 files.  They will be the oldest files. We scan
	// each generation in descending break once we see a file less than 4.
	end := 0
	start := 0
	for i, g := range generations {
		if g.level() <= 3 {
			break
		}
		end = i + 1
	}

	// As compactions run, the oldest files get bigger.  We don't want to re-compact them during
	// this planning if they are maxed out so skip over any we see.
	var hasTombstones bool
	for i, g := range generations[:end] {
		if g.hasTombstones() {
			hasTombstones = true
		}

		if hasTombstones {
			continue
		}

		// Skip the file if it's over the max size and contains a full block or the generation is split
		// over multiple files.  In the latter case, that would mean the data in the file spilled over
		// the 2GB limit.
		if g.size() > uint64(maxTSMFileSize) && c.FileStore.BlockCount(g.files[0].Path, 1) == DefaultMaxPointsPerBlock {
			start = i + 1
		}

		// This is an edge case that can happen after multiple compactions run.  The files at the beginning
		// can become larger faster than ones after them.  We want to skip those really big ones and just
		// compact the smaller ones until they are closer in size.
		if i > 0 {
			if g.size()*2 < generations[i-1].size() {
				start = i
				break
			}
		}
	}

	// step is how may files to compact in a group.  We want to clamp it at 4 but also stil
	// return groups smaller than 4.
	step := 4
	if step > end {
		step = end
	}

	// slice off the generations that we'll examine
	generations = generations[start:end]

	// Loop through the generations in groups of size step and see if we can compact all (or
	// some of them as group)
	groups := []tsmGenerations{}
	for i := 0; i < len(generations); i += step {
		var skipGroup bool
		startIndex := i

		for j := i; j < i+step && j < len(generations); j++ {
			gen := generations[j]
			lvl := gen.level()

			// Skip compacting this group if there happens to be any lower level files in the
			// middle.  These will get picked up by the level compactors.
			if lvl <= 3 {
				skipGroup = true
				break
			}

			// Skip the file if it's over the max size and it contains a full block
			if gen.size() >= uint64(maxTSMFileSize) && c.FileStore.BlockCount(gen.files[0].Path, 1) == DefaultMaxPointsPerBlock && !gen.hasTombstones() {
				startIndex++
				continue
			}
		}

		if skipGroup {
			continue
		}

		endIndex := i + step
		if endIndex > len(generations) {
			endIndex = len(generations)
		}
		if endIndex-startIndex > 0 {
			groups = append(groups, generations[startIndex:endIndex])
		}
	}

	if len(groups) == 0 {
		return nil, 0
	}

	// With the groups, we need to evaluate whether the group as a whole can be compacted
	compactable := []tsmGenerations{}
	for _, group := range groups {
		// if we don't have enough generations to compact, skip it
		if len(group) < 4 && !group.hasTombstones() {
			continue
		}
		compactable = append(compactable, group)
	}

	// All the files to be compacted must be compacted in order.  We need to convert each
	// group to the actual set of files in that group to be compacted.
	var tsmFiles []CompactionGroup
	for _, c := range compactable {
		var cGroup CompactionGroup
		for _, group := range c {
			for _, f := range group.files {
				cGroup = append(cGroup, f.Path)
			}
		}
		sort.Strings(cGroup)
		tsmFiles = append(tsmFiles, cGroup)
	}

	if !c.acquire(tsmFiles) {
		return nil, int64(len(tsmFiles))
	}
	return tsmFiles, int64(len(tsmFiles))
}

// findGenerations groups all the TSM files by generation based
// on their filename, then returns the generations in descending order (newest first).
// If skipInUse is true, tsm files that are part of an existing compaction plan
// are not returned.
func (c *DefaultPlanner) findGenerations(skipInUse bool) tsmGenerations {
	c.mu.Lock()
	defer c.mu.Unlock()

	last := c.lastFindGenerations
	lastGen := c.lastGenerations

	if !last.IsZero() && c.FileStore.LastModified().Equal(last) {
		return lastGen
	}

	genTime := c.FileStore.LastModified()
	tsmStats := c.FileStore.Stats()
	generations := make(map[int]*tsmGeneration, len(tsmStats))
	for _, f := range tsmStats {
		gen, _, _ := c.ParseFileName(f.Path)

		// Skip any files that are assigned to a current compaction plan
		if _, ok := c.filesInUse[f.Path]; skipInUse && ok {
			continue
		}

		group := generations[gen]
		if group == nil {
			group = newTsmGeneration(gen, c.ParseFileName)
			generations[gen] = group
		}
		group.files = append(group.files, f)
	}

	orderedGenerations := make(tsmGenerations, 0, len(generations))
	for _, g := range generations {
		orderedGenerations = append(orderedGenerations, g)
	}
	if !orderedGenerations.IsSorted() {
		sort.Sort(orderedGenerations)
	}

	c.lastFindGenerations = genTime
	c.lastGenerations = orderedGenerations

	return orderedGenerations
}

func (c *DefaultPlanner) acquire(groups []CompactionGroup) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// See if the new files are already in use
	for _, g := range groups {
		for _, f := range g {
			if _, ok := c.filesInUse[f]; ok {
				return false
			}
		}
	}

	// Mark all the new files in use
	for _, g := range groups {
		for _, f := range g {
			c.filesInUse[f] = struct{}{}
		}
	}
	return true
}

// Release removes the files reference in each compaction group allowing new plans
// to be able to use them.
func (c *DefaultPlanner) Release(groups []CompactionGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, g := range groups {
		for _, f := range g {
			delete(c.filesInUse, f)
		}
	}
}

// Compactor merges multiple TSM files into new files or
// writes a Cache into 1 or more TSM files.
type Compactor struct {
	Dir  string
	Size int

	FileStore interface {
		NextGeneration() int
		TSMReader(path string) *store.TSMReader
	}

	// RateLimit is the limit for disk writes for all concurrent compactions.
	RateLimit limiter.Rate

	formatFileName store.FormatFileNameFunc
	parseFileName  store.ParseFileNameFunc

	mu                 sync.RWMutex
	snapshotsEnabled   bool
	compactionsEnabled bool

	// lastSnapshotDuration is the amount of time the last snapshot took to complete.
	lastSnapshotDuration time.Duration

	snapshotLatencies *latencies

	// The channel to signal that any in progress snapshots should be aborted.
	snapshotsInterrupt chan struct{}
	// The channel to signal that any in progress level compactions should be aborted.
	compactionsInterrupt chan struct{}

	files map[string]struct{}
}

// NewCompactor returns a new instance of Compactor.
func NewCompactor() *Compactor {
	return &Compactor{
		formatFileName: store.DefaultFormatFileName,
		parseFileName:  store.DefaultParseFileName,
	}
}

func (c *Compactor) WithFormatFileNameFunc(formatFileNameFunc store.FormatFileNameFunc) {
	c.formatFileName = formatFileNameFunc
}

func (c *Compactor) WithParseFileNameFunc(parseFileNameFunc store.ParseFileNameFunc) {
	c.parseFileName = parseFileNameFunc
}

// Open initializes the Compactor.
func (c *Compactor) Open() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshotsEnabled || c.compactionsEnabled {
		return
	}

	c.snapshotsEnabled = true
	c.compactionsEnabled = true
	c.snapshotsInterrupt = make(chan struct{})
	c.compactionsInterrupt = make(chan struct{})
	c.snapshotLatencies = &latencies{values: make([]time.Duration, 4)}

	c.files = make(map[string]struct{})
}

// Close disables the Compactor.
func (c *Compactor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !(c.snapshotsEnabled || c.compactionsEnabled) {
		return
	}
	c.snapshotsEnabled = false
	c.compactionsEnabled = false
	if c.compactionsInterrupt != nil {
		close(c.compactionsInterrupt)
	}
	if c.snapshotsInterrupt != nil {
		close(c.snapshotsInterrupt)
	}
}

// DisableSnapshots disables the compactor from performing snapshots.
func (c *Compactor) DisableSnapshots() {
	c.mu.Lock()
	c.snapshotsEnabled = false
	if c.snapshotsInterrupt != nil {
		close(c.snapshotsInterrupt)
		c.snapshotsInterrupt = nil
	}
	c.mu.Unlock()
}

// EnableSnapshots allows the compactor to perform snapshots.
func (c *Compactor) EnableSnapshots() {
	c.mu.Lock()
	c.snapshotsEnabled = true
	if c.snapshotsInterrupt == nil {
		c.snapshotsInterrupt = make(chan struct{})
	}
	c.mu.Unlock()
}

// DisableSnapshots disables the compactor from performing compactions.
func (c *Compactor) DisableCompactions() {
	c.mu.Lock()
	c.compactionsEnabled = false
	if c.compactionsInterrupt != nil {
		close(c.compactionsInterrupt)
		c.compactionsInterrupt = nil
	}
	c.mu.Unlock()
}

// EnableCompactions allows the compactor to perform compactions.
func (c *Compactor) EnableCompactions() {
	c.mu.Lock()
	c.compactionsEnabled = true
	if c.compactionsInterrupt == nil {
		c.compactionsInterrupt = make(chan struct{})
	}
	c.mu.Unlock()
}

// WriteSnapshot writes a Cache snapshot to one or more new TSM files.
func (c *Compactor) WriteSnapshot(cache *memory.Cache, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.snapshotsEnabled
	intC := c.snapshotsInterrupt
	c.mu.RUnlock()

	if !enabled {
		return nil, errSnapshotsDisabled
	}

	start := time.Now()
	card := cache.Count()

	// Enable throttling if we have lower cardinality or snapshots are going fast.
	throttle := card < 3e6 && c.snapshotLatencies.avg() < 15*time.Second

	// Write snapshot concurrently if cardinality is relatively high.
	concurrency := card / 2e6
	if concurrency < 1 {
		concurrency = 1
	}

	// Special case very high cardinality, use max concurrency and don't throttle writes.
	if card >= 3e6 {
		concurrency = 4
		throttle = false
	}

	splits := cache.Split(concurrency)

	type res struct {
		files []string
		err   error
	}

	resC := make(chan res, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(sp *memory.Cache) {
			iter := NewCacheKeyIterator(sp, intC)
			defer func(iter KeyIterator) {
				_ = iter.Close()
			}(iter)
			files, err := c.writeNewFiles(c.FileStore.NextGeneration(), 0, nil, iter, throttle, logger)
			resC <- res{files: files, err: err}
		}(splits[i])
	}

	var err error
	files := make([]string, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		result := <-resC
		if result.err != nil {
			err = result.err
		}
		files = append(files, result.files...)
	}

	dur := time.Since(start).Truncate(time.Second)

	c.mu.Lock()

	// See if we were disabled while writing a snapshot
	enabled = c.snapshotsEnabled
	c.lastSnapshotDuration = dur
	c.snapshotLatencies.add(time.Since(start))
	c.mu.Unlock()

	if !enabled {
		return nil, errSnapshotsDisabled
	}

	return files, err
}

// compact writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) compact(fast bool, tsmFiles []string, logger *zap.Logger) ([]string, error) {
	size := c.Size
	if size <= 0 {
		size = DefaultMaxPointsPerBlock
	}

	c.mu.RLock()
	intC := c.compactionsInterrupt
	c.mu.RUnlock()

	// The new compacted files need to added to the max generation in the
	// set.  We need to find that max generation as well as the max sequence
	// number to ensure we write to the next unique location.
	var maxGeneration, maxSequence int
	for _, f := range tsmFiles {
		gen, seq, err := c.parseFileName(f)
		if err != nil {
			return nil, err
		}

		if gen > maxGeneration {
			maxGeneration = gen
			maxSequence = seq
		}

		if gen == maxGeneration && seq > maxSequence {
			maxSequence = seq
		}
	}

	// For each TSM file, create a TSM reader
	var trs []*store.TSMReader
	for _, file := range tsmFiles {
		select {
		case <-intC:
			return nil, errCompactionAborted{}
		default:
		}

		tr := c.FileStore.TSMReader(file)
		if tr == nil {
			// This would be a bug if this occurred as tsmFiles passed in should only be
			// assigned to one compaction at any one time.  A nil tr would mean the file
			// doesn't exist.
			return nil, errCompactionAborted{fmt.Errorf("bad plan: %s", file)}
		}
		defer tr.Unref() // inform that we're done with this reader when this method returns.
		trs = append(trs, tr)
	}

	if len(trs) == 0 {
		logger.Debug("No input files")
		return nil, nil
	}

	tsm, err := NewTSMBatchKeyIterator(size, fast, DefaultMaxSavedErrors, intC, tsmFiles, trs...)
	if err != nil {
		return nil, err
	}

	return c.writeNewFiles(maxGeneration, maxSequence, tsmFiles, tsm, true, logger)
}

// CompactFull writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) CompactFull(tsmFiles []string, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		return nil, errCompactionsDisabled
	}

	if !c.add(tsmFiles) {
		return nil, errCompactionInProgress{}
	}
	defer c.remove(tsmFiles)

	files, err := c.compact(false, tsmFiles, logger)

	// See if we were disabled while writing a snapshot
	c.mu.RLock()
	enabled = c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		if err := c.removeTmpFiles(files); err != nil {
			return nil, err
		}
		return nil, errCompactionsDisabled
	}

	return files, err
}

// CompactFast writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) CompactFast(tsmFiles []string, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		return nil, errCompactionsDisabled
	}

	if !c.add(tsmFiles) {
		return nil, errCompactionInProgress{}
	}
	defer c.remove(tsmFiles)

	files, err := c.compact(true, tsmFiles, logger)

	// See if we were disabled while writing a snapshot
	c.mu.RLock()
	enabled = c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		if err := c.removeTmpFiles(files); err != nil {
			return nil, err
		}
		return nil, errCompactionsDisabled
	}

	return files, err

}

// removeTmpFiles is responsible for cleaning up a compaction that
// was started, but then abandoned before the temporary files were dealt with.
func (c *Compactor) removeTmpFiles(files []string) error {
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction file: %v", err)
		}
	}
	return nil
}

// writeNewFiles writes from the iterator into new TSM files, rotating
// to a new file once it has reached the max TSM file size.
func (c *Compactor) writeNewFiles(generation, sequence int, src []string, iter KeyIterator, throttle bool, logger *zap.Logger) ([]string, error) {
	// These are the new TSM files written
	var files []string

	for {
		sequence++

		// New TSM files are written to a temp file and renamed when fully completed.
		fileName := filepath.Join(c.Dir, c.formatFileName(generation, sequence)+"."+store.TSMFileExtension+"."+store.TmpTSMFileExtension)
		logger.Debug("Compacting files", zap.Int("file_count", len(src)), zap.String("output_file", fileName))

		// Write as much as possible to this file
		err := c.write(fileName, iter, throttle, logger)

		// We've hit the max file limit and there is more to write.  Create a new file
		// and continue.
		if errors.Is(err, errMaxFileExceeded) || errors.Is(err, store.ErrMaxBlocksExceeded) {
			files = append(files, fileName)
			logger.Debug("file size or block count exceeded, opening another output file", zap.String("output_file", fileName))
			continue
		} else if errors.Is(err, store.ErrNoValues) {
			logger.Debug("Dropping empty file", zap.String("output_file", fileName))
			// If the file only contained tombstoned entries, then it would be a 0 length
			// file that we can drop.
			if err := os.RemoveAll(fileName); err != nil {
				return nil, err
			}
			break
		} else if _, ok := err.(errCompactionInProgress); ok {
			// Don't clean up the file as another compaction is using it.  This should not happen as the
			// planner keeps track of which files are assigned to compaction plans now.
			return nil, err
		} else if err != nil {
			// We hit an error and didn't finish the compaction.  Abort.
			// Remove any tmp files we already completed
			// discard later errors to return the first one from the write() call
			for _, f := range files {
				_ = os.RemoveAll(f)
			}
			// Remove the temp file
			// discard later errors to return the first one from the write() call
			_ = os.RemoveAll(fileName)
			return nil, err
		}

		files = append(files, fileName)
		break
	}

	return files, nil
}

func (c *Compactor) write(path string, iter KeyIterator, throttle bool, logger *zap.Logger) (err error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return errCompactionInProgress{err: err}
	}

	// syncingWriter ensures that whatever we wrap the above file descriptor in
	// it will always be able to be synced by the tsm writer, since it does
	// type assertions to attempt to sync.
	type syncingWriter interface {
		io.Writer
		Sync() error
	}

	// Create the write for the new TSM file.
	var (
		w           store.SSTWriter
		limitWriter syncingWriter = fd
	)

	if c.RateLimit != nil && throttle {
		limitWriter = limiter.NewWriterWithRate(fd, c.RateLimit)
	}

	// Use a disk based TSM buffer if it looks like we might create a big index
	// in memory.
	if iter.EstimatedIndexSize() > 64*1024*1024 {
		w, err = store.NewTSMWriterWithDiskBuffer(limitWriter)
		if err != nil {
			return err
		}
	} else {
		w, err = store.NewTSMWriter(limitWriter)
		if err != nil {
			return err
		}
	}

	defer func() {
		closeErr := w.Close()
		if err == nil {
			err = closeErr
		}

		// Check for errors where we should not remove the file
		_, inProgress := err.(errCompactionInProgress)
		maxBlocks := err == store.ErrMaxBlocksExceeded
		maxFileSize := err == errMaxFileExceeded
		if inProgress || maxBlocks || maxFileSize {
			return
		}

		if err != nil {
			_ = w.Remove()
		}
	}()

	lastLogSize := w.Size()
	for iter.Next() {
		c.mu.RLock()
		enabled := c.snapshotsEnabled || c.compactionsEnabled
		c.mu.RUnlock()

		if !enabled {
			return errCompactionAborted{}
		}
		// Each call to read returns the next sorted key (or the prior one if there are
		// more values to write).  The size of values will be less than or equal to our
		// chunk size (1000)
		key, minTime, maxTime, block, err := iter.Read()
		if err != nil {
			return err
		}

		if minTime > maxTime {
			return fmt.Errorf("invalid index entry for block. min=%d, max=%d", minTime, maxTime)
		}

		// Write the key and value
		if err := w.WriteBlock(key, minTime, maxTime, block); err == store.ErrMaxBlocksExceeded {
			if err := w.WriteIndex(); err != nil {
				return err
			}
			return err
		} else if err != nil {
			return err
		}

		// If we have a max file size configured and we're over it, close out the file
		// and return the error.
		if w.Size() > maxTSMFileSize {
			if err := w.WriteIndex(); err != nil {
				return err
			}

			return errMaxFileExceeded
		} else if (w.Size() - lastLogSize) > logEvery {
			logger.Debug("Compaction progress", zap.String("output_file", path), zap.Uint32("size", w.Size()))
			lastLogSize = w.Size()
		}
	}

	// Were there any errors encountered during iteration?
	if err := iter.Err(); err != nil {
		return err
	}

	// We're all done.  Close out the file.
	if err := w.WriteIndex(); err != nil {
		return err
	}
	logger.Debug("Compaction finished", zap.String("output_file", path), zap.Uint32("size", w.Size()))
	return nil
}

func (c *Compactor) add(files []string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// See if the new files are already in use
	for _, f := range files {
		if _, ok := c.files[f]; ok {
			return false
		}
	}

	// Mark all the new files in use
	for _, f := range files {
		c.files[f] = struct{}{}
	}
	return true
}

func (c *Compactor) remove(files []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range files {
		delete(c.files, f)
	}
}

type TSMErrors []error

func (t TSMErrors) Error() string {
	e := []string{}
	for _, v := range t {
		e = append(e, v.Error())
	}
	return strings.Join(e, ", ")
}

type block struct {
	key              []byte
	minTime, maxTime int64
	typ              byte
	b                []byte
	tombstones       []store.TimeRange

	// readMin, readMax are the timestamps range of values have been
	// read and encoded from this block.
	readMin, readMax int64
}

func (b *block) overlapsTimeRange(min, max int64) bool {
	return b.minTime <= max && b.maxTime >= min
}

func (b *block) read() bool {
	return b.readMin <= b.minTime && b.readMax >= b.maxTime
}

func (b *block) markRead(min, max int64) {
	if min < b.readMin {
		b.readMin = min
	}

	if max > b.readMax {
		b.readMax = max
	}
}

func (b *block) partiallyRead() bool {
	// If readMin and readMax are still the initial values, nothing has been read.
	if b.readMin == int64(math.MaxInt64) && b.readMax == int64(math.MinInt64) {
		return false
	}
	return b.readMin != b.minTime || b.readMax != b.maxTime
}

type blocks []*block

func (a blocks) Len() int { return len(a) }

func (a blocks) Less(i, j int) bool {
	cmp := bytes.Compare(a[i].key, a[j].key)
	if cmp == 0 {
		return a[i].minTime < a[j].minTime && a[i].maxTime < a[j].minTime
	}
	return cmp < 0
}

func (a blocks) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type tsmGenerations []*tsmGeneration

func (a tsmGenerations) Len() int           { return len(a) }
func (a tsmGenerations) Less(i, j int) bool { return a[i].id < a[j].id }
func (a tsmGenerations) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tsmGenerations) hasTombstones() bool {
	for _, g := range a {
		if g.hasTombstones() {
			return true
		}
	}
	return false
}

func (a tsmGenerations) level() int {
	var level int
	for _, g := range a {
		lev := g.level()
		if lev > level {
			level = lev
		}
	}
	return level
}

func (a tsmGenerations) chunk(size int) []tsmGenerations {
	var chunks []tsmGenerations
	for len(a) > 0 {
		if len(a) >= size {
			chunks = append(chunks, a[:size])
			a = a[size:]
		} else {
			chunks = append(chunks, a)
			a = a[len(a):]
		}
	}
	return chunks
}

func (a tsmGenerations) IsSorted() bool {
	if len(a) == 1 {
		return true
	}

	for i := 1; i < len(a); i++ {
		if a.Less(i, i-1) {
			return false
		}
	}
	return true
}

type latencies struct {
	i      int
	values []time.Duration
}

func (l *latencies) add(t time.Duration) {
	l.values[l.i%len(l.values)] = t
	l.i++
}

func (l *latencies) avg() time.Duration {
	var n int64
	var sum time.Duration
	for _, v := range l.values {
		if v == 0 {
			continue
		}
		sum += v
		n++
	}

	if n > 0 {
		return time.Duration(int64(sum) / n)
	}
	return time.Duration(0)
}
