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
	"equinox/storage/store"
	"errors"
	"go.uber.org/zap"
	"os"
	"sync/atomic"
	"time"
)

type compactionStrategy struct {
	group compactor.CompactionGroup

	fast  bool
	level int

	logger    *zap.Logger
	compactor *compactor.Compactor
	fileStore *store.FileStore

	engine *Engine
}

// Apply concurrently compacts all the groups in a compaction strategy.
func (s *compactionStrategy) Apply() {
	s.compactGroup()
}

// compactGroup executes the compaction strategy against a single CompactionGroup.
func (s *compactionStrategy) compactGroup() {
	group := s.group

	s.logger.Info("Beginning compaction", zap.Int("tsm1_files_n", len(group)))
	for i, f := range group {
		s.logger.Info("Compacting file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
	}

	var (
		err   error
		files []string
	)
	if s.fast {
		files, err = s.compactor.CompactFast(group, s.logger)
	} else {
		files, err = s.compactor.CompactFull(group, s.logger)
	}

	if err != nil {
		var errCompactionInProgress compactor.ErrCompactionInProgress
		inProgress := errors.As(err, &errCompactionInProgress)
		if errors.Is(err, compactor.ErrCompactionsDisabled) || inProgress {
			s.logger.Info("Aborted compaction", zap.Error(err))
			if errors.As(err, &errCompactionInProgress) {
				time.Sleep(time.Second)
			}
			return
		}

		s.logger.Warn("Error compacting TSM files", zap.Error(err))

		// We hit a bad TSM file - rename so the next compaction can proceed.
		var errBlockRead compactor.ErrBlockRead
		if errors.As(err, &errBlockRead) {
			path := err.(compactor.ErrBlockRead).File
			s.logger.Info("Renaming a corrupt TSM file due to compaction error", zap.Error(err))
			if err := s.fileStore.ReplaceWithCallback([]string{path}, nil, nil); err != nil {
				s.logger.Info("Error removing bad TSM file", zap.Error(err))
			} else if e := os.Rename(path, path+"."+store.BadTSMFileExtension); e != nil {
				s.logger.Info("Error renaming corrupt TSM file", zap.Error((err)))
			}
		}
		time.Sleep(time.Second)
		return
	}

	if err := s.fileStore.ReplaceWithCallback(group, files, nil); err != nil {
		s.logger.Info("Error replacing new TSM files", zap.Error(err))
		time.Sleep(time.Second)

		// Remove the new snapshot files. We will try again.
		for _, file := range files {
			if err := os.Remove(file); err != nil {
				s.logger.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
			}
		}
		return
	}

	for i, f := range files {
		s.logger.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
	}
	s.logger.Info("Finished compacting files",
		zap.Int("tsm1_files_n", len(files)))
}

type compactionCounter struct {
	l1       int64
	l2       int64
	l3       int64
	full     int64
	optimize int64
}

func (c *compactionCounter) countForLevel(l int) *int64 {
	switch l {
	case 1:
		return &c.l1
	case 2:
		return &c.l2
	case 3:
		return &c.l3
	}
	return nil
}

var defaultWeights = [4]float64{0.4, 0.3, 0.2, 0.1}

type scheduler struct {
	maxConcurrency    int
	activeCompactions *compactionCounter

	// queues is the depth of work pending for each compaction level
	queues  [4]int
	weights [4]float64
}

func newScheduler(activeCompactions *compactionCounter, maxConcurrency int) *scheduler {
	return &scheduler{
		activeCompactions: activeCompactions,
		maxConcurrency:    maxConcurrency,
		weights:           defaultWeights,
	}
}

func (s *scheduler) setDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *scheduler) next() (int, bool) {
	level1Running := int(atomic.LoadInt64(&s.activeCompactions.l1))
	level2Running := int(atomic.LoadInt64(&s.activeCompactions.l2))
	level3Running := int(atomic.LoadInt64(&s.activeCompactions.l3))
	level4Running := int(atomic.LoadInt64(&s.activeCompactions.full) + atomic.LoadInt64(&s.activeCompactions.optimize))

	if level1Running+level2Running+level3Running+level4Running >= s.maxConcurrency {
		return 0, false
	}

	var (
		level    int
		runnable bool
	)

	loLimit, _ := s.limits()

	end := len(s.queues)
	if level3Running+level4Running >= loLimit && s.maxConcurrency-(level1Running+level2Running) == 0 {
		end = 2
	}

	var weight float64
	for i := 0; i < end; i++ {
		if float64(s.queues[i])*s.weights[i] > weight {
			level, runnable = i+1, true
			weight = float64(s.queues[i]) * s.weights[i]
		}
	}
	return level, runnable
}

func (s *scheduler) limits() (int, int) {
	hiLimit := s.maxConcurrency * 4 / 5
	loLimit := (s.maxConcurrency / 5) + 1
	if hiLimit == 0 {
		hiLimit = 1
	}

	if loLimit == 0 {
		loLimit = 1
	}

	return loLimit, hiLimit
}
