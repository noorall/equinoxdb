/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package storage

import (
	"equinox/storage/compactor"
	"equinox/storage/metric"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

func (e *Engine) enableLevelCompactions(wait bool) {
	// If we don't need to wait, see if we're already enabled
	if !wait {
		e.mu.RLock()
		if e.compactionDone != nil {
			e.mu.RUnlock()
			return
		}
		e.mu.RUnlock()
	}

	e.mu.Lock()
	if wait {
		e.levelWorkers -= 1
	}
	if e.levelWorkers != 0 || e.compactionDone != nil {
		// still waiting on more workers or already enabled
		e.mu.Unlock()
		return
	}

	// last one to enable, start things back up
	e.compactor.EnableCompactions()
	e.compactionDone = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	e.compactionWG = wg
	e.mu.Unlock()

	go func() { defer wg.Done(); e.compact(wg) }()
}

// disableLevelCompactions will stop level compactions before returning.
//
// If 'wait' is set to true, then a corresponding call to enableLevelCompactions(true) will be
// required before level compactions will start back up again.
func (e *Engine) disableLevelCompactions(wait bool) {
	e.mu.Lock()
	old := e.levelWorkers
	if wait {
		e.levelWorkers += 1
	}

	// Hold onto the current done channel so we can wait on it if necessary
	waitCh := e.compactionDone
	wg := e.compactionWG

	if old == 0 && e.compactionDone != nil {
		// It's possible we have closed the done channel and released the lock and another
		// goroutine has attempted to disable compactions.  We're current in the process of
		// disabling them so check for this and wait until the original completes.
		select {
		case <-e.compactionDone:
			e.mu.Unlock()
			return
		default:
		}

		// Prevent new compactions from starting
		e.compactor.DisableCompactions()

		// Stop all background compaction goroutines
		close(e.compactionDone)
		e.mu.Unlock()
		wg.Wait()

		// Signal that all goroutines have exited.
		e.mu.Lock()
		e.compactionDone = nil
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	// Compaction were already disabled.
	if waitCh == nil {
		return
	}

	// We were not the first caller to disable compactions and they were in the process
	// of being disabled.  Wait for them to complete before returning.
	<-waitCh
	wg.Wait()
}

func (e *Engine) compact(wg *sync.WaitGroup) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	for {
		e.mu.RLock()
		quit := e.compactionDone
		e.mu.RUnlock()

		select {
		case <-quit:
			return

		case <-t.C:

			// Find our compaction plans
			level1Groups, len1 := e.compactionPlan.PlanLevel(1)
			level2Groups, len2 := e.compactionPlan.PlanLevel(2)
			level3Groups, len3 := e.compactionPlan.PlanLevel(3)
			level4Groups, len4 := e.compactionPlan.Plan(e.LastModified())

			e.compactionStats.Queued.With(prometheus.Labels{metric.LevelKey: metric.LevelFull}).Set(float64(len4))

			// If no full compactions are need, see if an optimize is needed
			if len(level4Groups) == 0 {
				level4Groups, len4 = e.compactionPlan.PlanOptimize()
				e.compactionStats.Queued.With(prometheus.Labels{metric.LevelKey: metric.LevelOpt}).Set(float64(len4))
			}

			// Update the level plan queue stats
			// For stats, use the length needed, even if the lock was
			// not acquired
			e.compactionStats.Queued.With(prometheus.Labels{metric.LevelKey: metric.Level1}).Set(float64(len1))
			e.compactionStats.Queued.With(prometheus.Labels{metric.LevelKey: metric.Level2}).Set(float64(len2))
			e.compactionStats.Queued.With(prometheus.Labels{metric.LevelKey: metric.Level3}).Set(float64(len3))

			// Set the queue depths on the scheduler
			// Use the real queue depth, dependent on acquiring
			// the file locks.
			e.scheduler.setDepth(1, len(level1Groups))
			e.scheduler.setDepth(2, len(level2Groups))
			e.scheduler.setDepth(3, len(level3Groups))
			e.scheduler.setDepth(4, len(level4Groups))

			// Find the next compaction that can run and try to kick it off
			if level, runnable := e.scheduler.next(); runnable {
				switch level {
				case 1:
					if e.compactLevel(level1Groups[0], 1, false, wg) {
						level1Groups = level1Groups[1:]
					}
				case 2:
					if e.compactLevel(level2Groups[0], 2, false, wg) {
						level2Groups = level2Groups[1:]
					}
				case 3:
					if e.compactLevel(level3Groups[0], 3, true, wg) {
						level3Groups = level3Groups[1:]
					}
				case 4:
					if e.compactFull(level4Groups[0], wg) {
						level4Groups = level4Groups[1:]
					}
				}
			}

			// Release all the plans we didn't start.
			e.compactionPlan.Release(level1Groups)
			e.compactionPlan.Release(level2Groups)
			e.compactionPlan.Release(level3Groups)
			e.compactionPlan.Release(level4Groups)
		}
	}
}

// compactLevel kicks off compactions using the level strategy. It returns
// true if the compaction was started
func (e *Engine) compactLevel(grp compactor.CompactionGroup, level int, fast bool, wg *sync.WaitGroup) bool {
	s := e.levelCompactionStrategy(grp, fast, level)
	if s == nil {
		return false
	}

	if e.compactionLimiter.TryTake() {
		{
			val := atomic.AddInt64(e.activeCompactions.countForLevel(level), 1)
			e.compactionStats.Active.With(metric.LabelForLevel(level)).Set(float64(val))
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				val := atomic.AddInt64(e.activeCompactions.countForLevel(level), -1)
				e.compactionStats.Active.With(metric.LabelForLevel(level)).Set(float64(val))
			}()

			defer e.compactionLimiter.Release()
			s.Apply()
			// Release the files in the compaction plan
			e.compactionPlan.Release([]compactor.CompactionGroup{s.group})
		}()
		return true
	}

	// Return the unused plans
	return false
}

// compactFull kicks off full and optimize compactions using the lo priority policy. It returns
// the plans that were not able to be started.
func (e *Engine) compactFull(grp compactor.CompactionGroup, wg *sync.WaitGroup) bool {
	s := e.fullCompactionStrategy(grp, false)
	if s == nil {
		return false
	}

	// Try the lo priority limiter, otherwise steal a little from the high priority if we can.
	if e.compactionLimiter.TryTake() {
		{
			val := atomic.AddInt64(&e.activeCompactions.full, 1)
			e.compactionStats.Active.With(prometheus.Labels{metric.LevelKey: metric.LevelFull}).Set(float64(val))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				val := atomic.AddInt64(&e.activeCompactions.full, -1)
				e.compactionStats.Active.With(prometheus.Labels{metric.LevelKey: metric.LevelFull}).Set(float64(val))
			}()
			defer e.compactionLimiter.Release()
			s.Apply()
			// Release the files in the compaction plan
			e.compactionPlan.Release([]compactor.CompactionGroup{s.group})
		}()
		return true
	}
	return false
}

// levelCompactionStrategy returns a compactionStrategy for the given level.
// It returns nil if there are no SST files to compact.
func (e *Engine) levelCompactionStrategy(group compactor.CompactionGroup, fast bool, level int) *compactionStrategy {
	label := metric.LabelForLevel(level)
	return &compactionStrategy{
		group:               group,
		logger:              e.logger.With(zap.Int("sst1_level", level), zap.String("sst1_strategy", "level")),
		fileStore:           e.filestore,
		compactor:           e.compactor,
		fast:                fast,
		engine:              e,
		level:               level,
		errorStat:           e.compactionStats.Failed.With(label),
		durationSecondsStat: e.compactionStats.Duration.With(label),
	}
}

// fullCompactionStrategy returns a compactionStrategy for higher level generations of SST files.
// It returns nil if there are no SST files to compact.
func (e *Engine) fullCompactionStrategy(group compactor.CompactionGroup, optimize bool) *compactionStrategy {
	s := &compactionStrategy{
		group:     group,
		logger:    e.logger.With(zap.String("sst1_strategy", "full"), zap.Bool("sst1_optimize", optimize)),
		fileStore: e.filestore,
		compactor: e.compactor,
		fast:      optimize,
		engine:    e,
		level:     4,
	}
	plabel := prometheus.Labels{metric.LevelKey: metric.LevelFull}
	if optimize {
		plabel = prometheus.Labels{metric.LevelKey: metric.LevelOpt}
	}
	s.errorStat = e.compactionStats.Failed.With(plabel)
	s.durationSecondsStat = e.compactionStats.Duration.With(plabel)
	return s
}
