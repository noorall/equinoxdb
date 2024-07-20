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

import "fmt"

var (
	ErrMaxFileExceeded     = fmt.Errorf("max File exceeded")
	ErrSnapshotsDisabled   = fmt.Errorf("snapshots disabled")
	ErrCompactionsDisabled = fmt.Errorf("compactions disabled")
)

type ErrCompactionInProgress struct {
	err error
}

// Error returns the string representation of the error, to satisfy the error interface.
func (e ErrCompactionInProgress) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction in progress: %s", e.err)
	}
	return "compaction in progress"
}

type ErrCompactionAborted struct {
	err error
}

func (e ErrCompactionAborted) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction aborted: %s", e.err)
	}
	return "compaction aborted"
}

type ErrBlockRead struct {
	File string
	err  error
}

func (e ErrBlockRead) Error() string {
	if e.err != nil {
		return fmt.Sprintf("block read error on %s: %s", e.File, e.err)
	}
	return fmt.Sprintf("block read error on %s", e.File)
}
