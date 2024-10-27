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

package config

import (
	"equinox/pkg/limiter"
	"go.uber.org/zap"
	"runtime"
	"time"
)

const (
	DefaultCompactThroughput            = 48 * 1024 * 1024
	DefaultCompactThroughputBurst       = 48 * 1024 * 1024
	DefaultCompactFullWriteColdDuration = 4 * time.Hour
	DefaultNumMemTables                 = 4
	DefaultMaxMemTableSize              = 64 << 20
	DefaultMaxValueFileSize             = 1<<30 - 1
	DefaultValueFileMaxEntries          = 4096
)

type Option struct {
	Dir             string
	SyncWrite       bool
	NumMemTables    int
	MaxMemTableSize int

	OpenLimiter limiter.Fixed

	CompactionDisabled           bool
	CompactionLimiter            limiter.Fixed
	CompactionThroughputLimiter  limiter.Rate
	CompactFullWriteColdDuration time.Duration

	SeparateDeciderEnabled bool
	SeparateFactor         float64
	SeparateThreshold      int
	SeparateZetaStep       int
	SeparateDistMu         float64
	SeparateDistSigma      float64

	ValueFileMaxSize            int
	ValueFileMaxEntries         int
	ValueFileOpenLimiter        limiter.Fixed
	ValueFileParallelismLimiter limiter.Fixed

	Logger *zap.Logger
}

func NewOption() Option {
	return Option{
		SyncWrite:       true,
		NumMemTables:    DefaultNumMemTables,
		MaxMemTableSize: DefaultMaxMemTableSize,

		OpenLimiter:                  limiter.NewFixed(runtime.GOMAXPROCS(0)),
		CompactionLimiter:            limiter.NewFixed(runtime.GOMAXPROCS(0)),
		CompactionThroughputLimiter:  limiter.NewRate(DefaultCompactThroughput, DefaultCompactThroughputBurst),
		CompactFullWriteColdDuration: DefaultCompactFullWriteColdDuration,

		ValueFileMaxSize:            DefaultMaxValueFileSize,
		ValueFileMaxEntries:         DefaultValueFileMaxEntries,
		ValueFileOpenLimiter:        limiter.NewFixed(runtime.GOMAXPROCS(0)),
		ValueFileParallelismLimiter: limiter.NewFixed(runtime.GOMAXPROCS(0)),

		SeparateDeciderEnabled: false,
		SeparateFactor:         1.0,
		SeparateThreshold:      20,
		SeparateDistMu:         40.0,
		SeparateDistSigma:      5.0,
		SeparateZetaStep:       10,

		Logger: zap.NewNop(),
	}
}
