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

package pool

// BytesHoldPool is a pool of byte slices that can be re-used.  Slices in
// this pool will not be garbage collected when not in use.
type BytesHoldPool struct {
	pool chan []byte
}

// NewBytesHoldPool returns a BytesHoldPool pool with capacity for max byte slices
// to be pool.
func NewBytesHoldPool(max int) *BytesHoldPool {
	return &BytesHoldPool{
		pool: make(chan []byte, max),
	}
}

// Get returns a byte slice size with at least sz capacity. Items
// returned may not be in the zero state and should be reset by the
// caller.
func (p *BytesHoldPool) Get(sz int) []byte {
	var c []byte
	select {
	case c = <-p.pool:
	default:
		return make([]byte, sz)
	}

	if cap(c) < sz {
		return make([]byte, sz)
	}

	return c[:sz]
}

// Put returns a slice back to the pool.  If the pool is full, the byte
// slice is discarded.
func (p *BytesHoldPool) Put(c []byte) {
	select {
	case p.pool <- c:
	default:
	}
}
