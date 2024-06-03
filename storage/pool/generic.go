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

// Generic is a pool of types that can be re-used.  Items in
// this pool will not be garbage collected when not in use.
type Generic struct {
	pool chan interface{}
	fn   func(sz int) interface{}
}

// NewGeneric returns a Generic pool with capacity for max items
// to be pool.
func NewGeneric(max int, fn func(sz int) interface{}) *Generic {
	return &Generic{
		pool: make(chan interface{}, max),
		fn:   fn,
	}
}

// Get returns a item from the pool or a new instance if the pool
// is empty.  Items returned may not be in the zero state and should
// be reset by the caller.
func (p *Generic) Get(sz int) interface{} {
	var c interface{}
	select {
	case c = <-p.pool:
	default:
		c = p.fn(sz)
	}

	return c
}

// Put returns an item back to the pool.  If the pool is full, the item
// is discarded.
func (p *Generic) Put(c interface{}) {
	select {
	case p.pool <- c:
	default:
	}
}
