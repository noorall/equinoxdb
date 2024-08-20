/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE File
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this File
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this File except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package compactor

import (
	"equinox/storage/store"
	"sort"
	"sync"
	"time"
)

// CompactionGroup represents a list of files eligible to be compacted together.
type CompactionGroup []string

// CompactionPlanner determines what SST files and WAL segments to include in a
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
// multiple generations of SST files into larger files in stages.  It attempts
// to minimize the number of SST files on disk while rolling up a bounder number
// of files.
type DefaultPlanner struct {
	FileStore FileStore

	// compactFullWriteColdDuration specifies the length of time after
	// which if no writes have been committed to the WAL, the engine will
	// do a full compaction of the SST files in this shard. This duration
	// should always be greater than the CacheFlushWriteColdDuration
	compactFullWriteColdDuration time.Duration

	// lastPlanCheck is the last time Plan was called
	lastPlanCheck time.Time

	mu sync.RWMutex
	// lastFindGenerations is the last time findGenerations was run
	lastFindGenerations time.Time

	// lastGenerations is the last set of generations found by findGenerations
	lastGenerations SstGenerations

	// forceFull causes the next full plan requests to plan any files
	// that may need to be compacted.  Normally, these files are skipped and scheduled
	// infrequently as the plans are more expensive to run.
	forceFull bool

	// filesInUse is the set of files that have been returned as part of a plan and might
	// be being compacted.  Two plans should not return the same File at any given time.
	filesInUse map[string]struct{}
}

type FileStore interface {
	Stats() []store.FileStat
	LastModified() time.Time
	BlockCount(path string, idx int) int
	ParseFileName(path string) (int, int, error)
}

func NewDefaultPlanner(fs FileStore, writeColdDuration time.Duration) *DefaultPlanner {
	return &DefaultPlanner{
		FileStore:                    fs,
		compactFullWriteColdDuration: writeColdDuration,
		filesInUse:                   make(map[string]struct{}),
	}
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

// PlanLevel returns a set of SST files to rewrite for a specific level.
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
	// a generation conceptually as a single File even though it may be
	// split across several files in sequence.
	generations := c.findGenerations(true)

	// If there is only one generation and no tombstones, then there's nothing to
	// do.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Group each generation by level such that two adjacent generations in the same
	// level become part of the same group.
	var currentGen SstGenerations
	var groups []SstGenerations
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

		currentGen = SstGenerations{}
		currentGen = append(currentGen, cur)
	}

	if len(currentGen) > 0 {
		groups = append(groups, currentGen)
	}

	// Remove any groups in the wrong level
	var levelGroups []SstGenerations
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

// PlanOptimize returns all SST files if they are in different generations in order
// to optimize the index across SST files.  Each returned compaction group can be
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
	// a generation conceptually as a single File even though it may be
	// split across several files in sequence.
	generations := c.findGenerations(true)

	// If there is only one generation and no tombstones, then there's nothing to
	// do.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Group each generation by level such that two adjacent generations in the same
	// level become part of the same group.
	var currentGen SstGenerations
	var groups []SstGenerations
	for i := 0; i < len(generations); i++ {
		cur := generations[i]

		// Skip the File if it's over the max size and contains a full block and it does not have any tombstones
		if cur.count() > 2 && cur.size() > uint64(maxSSTFileSize) && c.FileStore.BlockCount(cur.files[0].Path, 1) == DefaultMaxPointsPerBlock && !cur.hasTombstones() {
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

		currentGen = SstGenerations{}
		currentGen = append(currentGen, cur)
	}

	if len(currentGen) > 0 {
		groups = append(groups, currentGen)
	}

	// Only optimize level 4 files since using lower-levels will collide
	// with the level planners
	var levelGroups []SstGenerations
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

// Plan returns a set of SST files to rewrite for level 4 or higher.  The planning returns
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

		var sstFiles []string
		var genCount int
		for i, group := range generations {
			var skip bool

			// Skip the File if it's over the max size and contains a full block and it does not have any tombstones
			if len(generations) > 2 && group.size() > uint64(maxSSTFileSize) && c.FileStore.BlockCount(group.files[0].Path, 1) == DefaultMaxPointsPerBlock && !group.hasTombstones() {
				skip = true
			}

			// We need to look at the level of the next File because it may need to be combined with this generation
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
				sstFiles = append(sstFiles, f.Path)
			}
			genCount += 1
		}
		sort.Strings(sstFiles)

		// Make sure we have more than 1 File and more than 1 generation
		if len(sstFiles) <= 1 || genCount <= 1 {
			return nil, 0
		}

		group := []CompactionGroup{sstFiles}
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

	// If there is only one generation, return early to avoid re-compacting the same File
	// over and over again.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Need to find the ending point for level 4 files.  They will be the oldest files. We scan
	// each generation in descending break once we see a File less than 4.
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

		// Skip the File if it's over the max size and contains a full block or the generation is split
		// over multiple files.  In the latter case, that would mean the data in the File spilled over
		// the 2GB limit.
		if g.size() > uint64(maxSSTFileSize) && c.FileStore.BlockCount(g.files[0].Path, 1) == DefaultMaxPointsPerBlock {
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
	groups := []SstGenerations{}
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

			// Skip the File if it's over the max size and it contains a full block
			if gen.size() >= uint64(maxSSTFileSize) && c.FileStore.BlockCount(gen.files[0].Path, 1) == DefaultMaxPointsPerBlock && !gen.hasTombstones() {
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
	compactable := []SstGenerations{}
	for _, group := range groups {
		// if we don't have enough generations to compact, skip it
		if len(group) < 4 && !group.hasTombstones() {
			continue
		}
		compactable = append(compactable, group)
	}

	// All the files to be compacted must be compacted in order.  We need to convert each
	// group to the actual set of files in that group to be compacted.
	var sstFiles []CompactionGroup
	for _, c := range compactable {
		var cGroup CompactionGroup
		for _, group := range c {
			for _, f := range group.files {
				cGroup = append(cGroup, f.Path)
			}
		}
		sort.Strings(cGroup)
		sstFiles = append(sstFiles, cGroup)
	}

	if !c.acquire(sstFiles) {
		return nil, int64(len(sstFiles))
	}
	return sstFiles, int64(len(sstFiles))
}

// findGenerations groups all the SST files by generation based
// on their filename, then returns the generations in descending order (newest first).
// If skipInUse is true, sst files that are part of an existing compaction plan
// are not returned.
func (c *DefaultPlanner) findGenerations(skipInUse bool) SstGenerations {
	c.mu.Lock()
	defer c.mu.Unlock()

	last := c.lastFindGenerations
	lastGen := c.lastGenerations

	if !last.IsZero() && c.FileStore.LastModified().Equal(last) {
		return lastGen
	}

	genTime := c.FileStore.LastModified()
	sstStats := c.FileStore.Stats()
	generations := make(map[int]*sstGeneration, len(sstStats))
	for _, f := range sstStats {
		gen, _, _ := c.ParseFileName(f.Path)

		// Skip any files that are assigned to a current compaction plan
		if _, ok := c.filesInUse[f.Path]; skipInUse && ok {
			continue
		}

		group := generations[gen]
		if group == nil {
			group = newSstGeneration(gen, c.ParseFileName)
			generations[gen] = group
		}
		group.files = append(group.files, f)
	}

	orderedGenerations := make(SstGenerations, 0, len(generations))
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
