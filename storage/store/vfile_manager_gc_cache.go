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

package store

import (
	"container/heap"
	"github.com/emirpasic/gods/v2/queues/priorityqueue"
	"sort"
	"sync"
	"time"
)

type valueNode struct {
	val   int64
	key   string
	index int
}

type dataValidationCache struct {
	sync.RWMutex
	capacity       int
	size           int
	ttl            int64
	cache          map[string]*priorityqueue.Queue[TimeRange]
	keyToValueNode map[string]*valueNode
	valueHeap      ValueHeap
}

func newDataValidationCache(capacity int) *dataValidationCache {
	return &dataValidationCache{
		capacity:       capacity,
		cache:          make(map[string]*priorityqueue.Queue[TimeRange]),
		keyToValueNode: make(map[string]*valueNode),
	}
}

func (c *dataValidationCache) Add(key string, timeRange TimeRange) {
	c.Lock()
	defer c.Unlock()
	if c.size >= c.capacity {
		c.cleanByTTL()
	}
	if c.size >= c.capacity {
		c.cleanByNodeValue()
	}
	q, ok := c.cache[key]
	if !ok {
		q = priorityqueue.NewWith(func(a, b TimeRange) int {
			return int(a.Max - b.Max)
		})
		c.keyToValueNode[key] = &valueNode{key: key}
		heap.Push(&c.valueHeap, c.keyToValueNode[key])
		c.cache[key] = q
	}
	q.Enqueue(timeRange)
	c.keyToValueNode[key].val += timeRange.Max - timeRange.Min
	heap.Fix(&c.valueHeap, c.keyToValueNode[key].index)
}

func (c *dataValidationCache) AddMulti(key string, timeRanges []TimeRange) {
	timeRanges = MergeTimeRanges(timeRanges)
	for _, t := range timeRanges {
		c.Add(key, t)
	}
}

func (c *dataValidationCache) Get(key string) []TimeRange {
	if data, ok := c.cache[key]; ok {
		return data.Values()
	}
	return nil
}

func (c *dataValidationCache) cleanByTTL() {
	t := time.Now().UnixNano() - c.ttl
	for _, v := range c.cache {
		for {
			tmp, ok := v.Peek()
			if ok && tmp.Max < t {
				v.Dequeue()
				c.size--
			} else {
				break
			}
		}
	}
}

func (c *dataValidationCache) cleanByNodeValue() {
	if c.valueHeap.Len() > 0 {
		node := heap.Pop(&c.valueHeap).(valueNode)
		c.size -= c.cache[node.key].Size()
		delete(c.cache, node.key)
	}
}

func MergeTimeRanges(timeRanges []TimeRange) []TimeRange {
	// Handle empty or single-element cases
	if len(timeRanges) == 0 {
		return []TimeRange{}
	}
	if len(timeRanges) == 1 {
		return timeRanges
	}

	// Sort timeRanges by Min, then by Max
	ranges := make([]TimeRange, len(timeRanges))
	copy(ranges, timeRanges)
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Min == ranges[j].Min {
			return ranges[i].Max < ranges[j].Max
		}
		return ranges[i].Min < ranges[j].Min
	})

	// Merge overlapping ranges
	var result []TimeRange
	curMin, curMax := ranges[0].Min, ranges[0].Max

	for i := 1; i < len(ranges); i++ {
		if ranges[i].Overlaps(curMin, curMax) {
			// Overlapping: extend curMax if necessary
			if ranges[i].Max > curMax {
				curMax = ranges[i].Max
			}
		} else {
			// Non-overlapping: append current range and start new one
			result = append(result, TimeRange{Min: curMin, Max: curMax})
			curMin, curMax = ranges[i].Min, ranges[i].Max
		}
	}

	// Append the last range
	result = append(result, TimeRange{Min: curMin, Max: curMax})

	return result
}

type ValueHeap []*valueNode

func (h ValueHeap) Len() int { return len(h) }

func (h ValueHeap) Less(i, j int) bool {
	return h[i].val < h[j].val
}

func (h ValueHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ValueHeap) Push(x any) {
	n := len(*h)
	node := x.(*valueNode)
	node.index = n
	*h = append(*h, node)
}

func (h *ValueHeap) Pop() any {
	old := *h
	n := len(old)
	node := old[n-1]
	node.index = -1
	*h = old[0 : n-1]
	return node
}
