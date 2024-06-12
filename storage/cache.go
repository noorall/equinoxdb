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
	"equinox/storage/codec"
	"equinox/storage/types"
	equinox "equinox/types"
	"math/rand"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight = 20
	mask      = 3
)

type CloseHandler func()
type Comparator func([]byte, []byte) int

type Cache struct {
	head *Node

	height     int32
	ref        int32
	Handler    CloseHandler
	comparator Comparator

	size atomic.Uint32
}

type Node struct {
	key []byte

	entry *types.Entry

	height uint16

	tower [maxHeight]*Node
}

func (n *Node) GetKey() []byte {
	return n.key
}

func (n *Node) GetEntry() *types.Entry {
	return n.entry
}

func (n *Node) setNexNode(height int, old *Node, new *Node) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(n.tower[height])), unsafe.Pointer(old), unsafe.Pointer(new))
}

func newNode(key []byte, value types.Values, height int) *Node {
	entry, _ := types.NewEntryValues(value)
	return &Node{
		height: uint16(height),
		key:    key,
		entry:  entry,
	}
}

func NewCache(cmp Comparator) *Cache {
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

func (c *Cache) Put(key []byte, values types.Values) error {
	c.size.Add(uint32(len(key) + values.Size()))

	oldHeight := c.getHeight()
	var prev [maxHeight + 1]*Node
	var next [maxHeight + 1]*Node

	prev[oldHeight] = c.head
	next[oldHeight] = nil

	for i := int(oldHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = c.getSplices(key, prev[i+1], i)
		// Found the exist key, merge
		if prev[i] == next[i] {
			return prev[i].entry.Add(values)
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

			// cas, try to add the new Node at level
			x.tower[i] = next[i]
			if prev[i].setNexNode(i, next[i], x) {
				// Insert the new Node between prev[i] and next[i].
				// Go to the next level.
				break
			}

			// CAS failed, there are some other nodes inserted concurrently among this Node inserting
			// So we need search splices for the Node again
			prev[i], next[i] = c.getSplices(key, prev[i], i)
			if prev[i] == next[i] {
				return prev[i].entry.Add(values)
			}
		}
	}
	return nil
}

func (c *Cache) Get(key []byte) *types.Entry {
	n := c.findGreaterOrEqual(key)

	if n == nil {
		return nil
	}

	return n.entry
}

func (c *Cache) Delete(key []byte) {
	n := c.findGreaterOrEqual(key)

	if n == nil {
		return
	}

	c.DecreaseSize(uint32(n.entry.Size()))
	n.entry.Clean()
}

func (c *Cache) DecreaseSize(delta uint32) {
	c.size.Add(^(delta - 1))
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

func (c *Cache) Deduplicate() {
	n := c.head.tower[0]
	for {
		if n == nil {
			return
		}
		n.entry.Deduplicate()
		n = n.tower[0]
	}
}

// TODO: optimize this part
func (c *Cache) Count() int {
	return len(c.GetAllNodes())
}

// TODO: implements this part
func (c *Cache) Split(n int) []*Cache {
	return []*Cache{c}
}

func (c *Cache) GetAllNodes() []*Node {
	var nodes []*Node
	head := c.head.tower[0]
	for {
		if head == nil {
			return nodes
		}
		nodes = append(nodes, head)
		head = head.tower[0]
	}
}

func (c *Cache) getSplices(key []byte, from *Node, level int) (*Node, *Node) {
	for {
		next := c.GetNext(from, level)
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

func (c *Cache) getFrom(key []byte, from *Node, level int) *Node {
	for {
		next := c.GetNext(from, level)
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

func (c *Cache) GetNext(node *Node, height int) *Node {
	return node.tower[height]
}

func (c *Cache) getHeight() int32 {
	return atomic.LoadInt32(&c.height)
}

// find the rightmost Node such that key < target
func (c *Cache) findLessThan(target []byte) *Node {
	curr, level := c.head, int(c.getHeight())-1

	for {
		next := c.GetNext(curr, level)

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

// find the leftmost Node such that key >= target
func (c *Cache) findGreaterOrEqual(target []byte) *Node {
	curr, level := c.head, int(c.getHeight())-1

	for {
		next := c.GetNext(curr, level)

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

func (c *Cache) findLast() *Node {
	curr := c.head
	level := int(c.getHeight()) - 1

	for {
		next := c.GetNext(curr, level)

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

type cacheBlock struct {
	k                []byte
	minTime, maxTime int64
	b                []byte
	err              error
}

type Iterator struct {
	c      *Cache
	n      *Node
	size   int
	nodes  []*Node
	blocks map[*Node][]cacheBlock
	ready  []chan struct{}
}

func NewIteratorForWrite(c *Cache) *Iterator {
	nodes := c.GetAllNodes()
	ready := make([]chan struct{}, len(nodes))
	for i := 0; i < len(nodes); i++ {
		ready[i] = make(chan struct{}, 1)
	}
	it := &Iterator{
		c:      c,
		size:   equinox.DefaultMaxPointsPerBlock,
		nodes:  nodes,
		ready:  ready,
		blocks: make(map[*Node][]cacheBlock),
	}
	go it.encode()
	return it
}

func (it *Iterator) encode() {
	concurrency := runtime.GOMAXPROCS(0)
	n := len(it.ready)

	chunkSize := 1
	idx := uint64(0)

	for i := 0; i < concurrency; i++ {
		// Run one goroutine per CPU and encode a section of the key space concurrently
		go func() {
			tEnc := codec.GetTimeEncoder(equinox.DefaultMaxPointsPerBlock)
			fEnc := codec.GetFloatEncoder(equinox.DefaultMaxPointsPerBlock)
			bEnc := codec.GetBooleanEncoder(equinox.DefaultMaxPointsPerBlock)
			uEnc := codec.GetUnsignedEncoder(equinox.DefaultMaxPointsPerBlock)
			sEnc := codec.GetStringEncoder(equinox.DefaultMaxPointsPerBlock)
			iEnc := codec.GetIntegerEncoder(equinox.DefaultMaxPointsPerBlock)

			defer codec.PutTimeEncoder(tEnc)
			defer codec.PutFloatEncoder(fEnc)
			defer codec.PutBooleanEncoder(bEnc)
			defer codec.PutUnsignedEncoder(uEnc)
			defer codec.PutStringEncoder(sEnc)
			defer codec.PutIntegerEncoder(iEnc)

			for {
				curIdx := int(atomic.AddUint64(&idx, uint64(chunkSize))) - chunkSize

				if curIdx >= n {
					break
				}

				curNode := it.nodes[curIdx]
				key := curNode.key
				values := curNode.entry.Values()

				for len(values) > 0 {

					end := len(values)
					if end > it.size {
						end = it.size
					}

					minTime, maxTime := values[0].UnixNano(), values[end-1].UnixNano()
					var b []byte
					var err error

					switch values[0].(type) {
					case types.FloatValue:
						b, err = codec.EncodeFloatBlockUsing(nil, values[:end], tEnc, fEnc)
					case types.IntegerValue:
						b, err = codec.EncodeIntegerBlockUsing(nil, values[:end], tEnc, iEnc)
					case types.UnsignedValue:
						b, err = codec.EncodeUnsignedBlockUsing(nil, values[:end], tEnc, uEnc)
					case types.BooleanValue:
						b, err = codec.EncodeBooleanBlockUsing(nil, values[:end], tEnc, bEnc)
					case types.StringValue:
						b, err = codec.EncodeStringBlockUsing(nil, values[:end], tEnc, sEnc)
					default:
						b, err = codec.EncodeValues(values[:end], nil)
					}

					values = values[end:]

					it.blocks[curNode] = append(it.blocks[curNode], cacheBlock{
						k:       key,
						minTime: minTime,
						maxTime: maxTime,
						b:       b,
						err:     err,
					})
				}
				// Notify this key is fully encoded
				it.ready[curIdx] <- struct{}{}
			}
		}()
	}
}

func (it *Iterator) Key() []byte {
	return it.n.key
}

func (it *Iterator) ReadBinaryValue() ([]byte, int64, int64, []byte, error) {
	blk := it.blocks[it.n][0]
	return blk.k, blk.minTime, blk.maxTime, blk.b, blk.err
}

func (it *Iterator) Value() *types.Entry {
	return it.n.entry
}

func (it *Iterator) Valid() bool {
	return it.n != nil
}

func (it *Iterator) Next() bool {
	if it.Valid() {
		if len(it.blocks[it.n]) > 0 {
			it.blocks[it.n] = it.blocks[it.n][1:]
			if len(it.blocks[it.n]) > 0 {
				return true
			}
		}
		it.n = it.c.GetNext(it.n, 0)
		return true
	}
	return false
}

func (it *Iterator) Prev() {
	if it.Valid() {
		it.n = it.c.findLessThan(it.Key())
	}
}

func (it *Iterator) Seek(target []byte) {
	if it.Valid() {
		it.n = it.c.findGreaterOrEqual(target)
	}
}

func (it *Iterator) SeekToFirst() {
	it.n = it.c.GetNext(it.c.head, 0)
}

func (it *Iterator) SeekToLast() {
	it.n = it.c.findLast()
}

func (it *Iterator) Close() error {
	it.c.Deref()
	return nil
}
