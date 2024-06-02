/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreementc.  See the NOTICE file
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

package types

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight = 20
	mask      = 3
)

type CloseHandler func()
type Comparator func([]byte, []byte) int

// Cache TODO: Non-thread-safe may need to be modified
type Cache struct {
	head *node

	height     int32
	ref        int32
	Handler    CloseHandler
	comparator Comparator

	size atomic.Uint32
}

type node struct {
	key []byte

	entry *Entry

	height uint16

	tower [maxHeight]*node
}

func (n *node) setNexNode(height int, old *node, new *node) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(n.tower[height])), unsafe.Pointer(old), unsafe.Pointer(new))
}

func newNode(key []byte, value Values, height int) *node {
	entry, _ := newEntryValues(value)
	return &node{
		height: uint16(height),
		key:    key,
		entry:  entry,
	}
}

func NewSkipList(cmp Comparator) *Cache {
	if cmp == nil {
		panic("Unset the comparator for Cache!")
	}

	head := newNode(nil, nil, maxHeight)

	return &Cache{head: head,
		height:     1,
		ref:        1,
		comparator: cmp,
	}
}

func (c *Cache) Put(key []byte, values Values) error {
	c.size.Add(uint32(len(key) + values.Size()))

	oldHeight := c.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node

	prev[oldHeight] = c.head
	next[oldHeight] = nil

	for i := int(oldHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = c.getSplices(key, prev[i+1], i)
		// Found the exist key, merge
		if prev[i] == next[i] {
			return prev[i].entry.add(values)
		}
	}

	height := c.randomHeight()

	// CAS
	oldHeight = c.getHeight()
	for height > oldHeight {
		if atomic.CompareAndSwapInt32(&c.height, oldHeight, height) {
			break
		}

		oldHeight = c.getHeight()
	}

	x := newNode(key, values, int(height))

	for i := 0; i < int(height); i++ {
		// CAS
		for {
			if prev[i] == nil {
				// Because the new height exceeds old height, maybe there are added some new nodes
				// We can search from c.head and add some new splices
				prev[i], next[i] = c.getSplices(key, c.head, i)
			}

			// cas, try to add the new node at level
			x.tower[i] = next[i]
			if prev[i].setNexNode(i, next[i], x) {
				// Insert the new node between prev[i] and next[i].
				// Go to the next level.
				break
			}

			// CAS failed, there are some other nodes inserted concurrently among this node inserting
			// So we need search splices for the node again
			prev[i], next[i] = c.getSplices(key, prev[i], i)
			if prev[i] == next[i] {
				return prev[i].entry.add(values)
			}
		}
	}
	return nil
}

func (c *Cache) Get(key []byte) ([]byte, *Entry) {
	n := c.findGreaterOrEqual(key)

	if n == nil {
		return key, nil
	}

	return n.key, n.entry
}

func (c *Cache) Empty() bool {
	return c.findLast() == nil
}

func (c *Cache) Size() uint32 {
	return c.size.Load()
}

func (c *Cache) Ref() {
	atomic.AddInt32(&c.ref, 1)
}

func (c *Cache) Deref() {
	n := atomic.AddInt32(&c.ref, -1)

	if n > 0 {
		return
	}

	if c.Handler != nil {
		c.Handler()
	}

	c.head = nil
}

func (c *Cache) Iterator() *Iterator {
	c.Ref()
	return &Iterator{c: c}
}

func (c *Cache) getSplices(key []byte, from *node, level int) (*node, *node) {
	for {
		next := c.getNext(from, level)
		if next == nil {
			return from, next
		}

		nextKey := next.key
		comp := c.comparator(nextKey, key)

		if comp < 0 {
			from = next
		} else if comp > 0 {
			return from, next
		} else {
			return next, next
		}
	}
}

func (c *Cache) getFrom(key []byte, from *node, level int) *node {
	for {
		next := c.getNext(from, level)
		if next == nil {
			return nil
		}

		nextKey := next.key
		comp := c.comparator(nextKey, key)

		if comp < 0 {
			from = next
		} else if comp > 0 {
			return nil
		} else {
			return from
		}
	}
}

func (c *Cache) getNext(node *node, height int) *node {
	return node.tower[height]
}

func (c *Cache) getHeight() int32 {
	return atomic.LoadInt32(&c.height)
}

// find the rightmost node such that key < target
func (c *Cache) findLessThan(target []byte) *node {
	curr, level := c.head, int(c.getHeight())-1

	for {
		next := c.getNext(curr, level)

		if next != nil && c.comparator(next.key, target) < 0 {
			curr = next
		} else if level == 0 {
			if curr == c.head {
				return nil
			}

			return curr
		} else {
			level--
		}
	}
}

// find the leftmost node such that key >= target
func (c *Cache) findGreaterOrEqual(target []byte) *node {
	curr, level := c.head, int(c.getHeight())-1

	for {
		next := c.getNext(curr, level)

		if next != nil && c.comparator(next.key, target) < 0 {
			curr = next
		} else if level == 0 {
			return next
		} else {
			level--
		}
	}
}

func (c *Cache) randomHeight() int32 {
	var h int32 = 1
	for h < maxHeight && (rand.Uint32()&mask) == 0 {
		h++
	}

	return h
}

func (c *Cache) findLast() *node {
	curr := c.head
	level := int(c.getHeight()) - 1

	for {
		next := c.getNext(curr, level)

		if next != nil {
			curr = next
		} else if level == 0 {
			if curr == c.head {
				return nil
			}

			return curr
		} else {
			level--
		}
	}
}

type Iterator struct {
	c *Cache
	n *node
}

func (i *Iterator) Key() []byte {
	return i.n.key
}

func (i *Iterator) Value() *Entry {
	return i.n.entry
}

func (i *Iterator) Valid() bool {
	return i.n != nil
}

func (i *Iterator) Next() {
	if i.Valid() {
		i.n = i.c.getNext(i.n, 0)
	}
}

func (i *Iterator) Prev() {
	if i.Valid() {
		i.n = i.c.findLessThan(i.Key())
	}
}

func (i *Iterator) Seek(target []byte) {
	if i.Valid() {
		i.n = i.c.findGreaterOrEqual(target)
	}
}

func (i *Iterator) SeekToFirst() {
	i.n = i.c.getNext(i.c.head, 0)
}

func (i *Iterator) SeekToLast() {
	i.n = i.c.findLast()
}

func (i *Iterator) Close() error {
	i.c.Deref()
	return nil
}
