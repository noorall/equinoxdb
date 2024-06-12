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

package compactor

// KeyIterator allows iteration over set of keys and values in sorted order.
type KeyIterator interface {
	// Next returns true if there are any values remaining in the iterator.
	Next() bool

	// Read returns the key, time range, and raw data for the next block,
	// or any error that occurred.
	Read() (key []byte, minTime int64, maxTime int64, data []byte, err error)

	// Close closes the iterator.
	Close() error

	// Err returns any errors encountered during iteration.
	Err() error

	// EstimatedIndexSize returns the estimated size of the index that would
	// be required to store all the series and entries in the KeyIterator.
	EstimatedIndexSize() int
}
