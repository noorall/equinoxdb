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

import (
	"equinox/storage"
	"equinox/storage/codec"
	"equinox/storage/types"
	equinox "equinox/types"
	"runtime"
	"sync/atomic"
)

type cacheKeyIterator struct {
	c         *storage.Cache
	n         *storage.Node
	size      int
	nodes     []*storage.Node
	blocks    map[*storage.Node][]cacheBlock
	ready     []chan struct{}
	interrupt chan struct{}
	err       error
}
type cacheBlock struct {
	k                []byte
	minTime, maxTime int64
	b                []byte
	err              error
}

func NewCacheKeyIterator(c *storage.Cache, interrupt chan struct{}) KeyIterator {
	nodes := c.GetAllNodes()
	ready := make([]chan struct{}, len(nodes))
	for i := 0; i < len(nodes); i++ {
		ready[i] = make(chan struct{}, 1)
	}
	it := &cacheKeyIterator{
		c:         c,
		size:      equinox.DefaultMaxPointsPerBlock,
		nodes:     nodes,
		ready:     ready,
		blocks:    make(map[*storage.Node][]cacheBlock),
		interrupt: interrupt,
	}
	it.c.Ref()
	go it.encode()
	return it
}
func (it *cacheKeyIterator) Read() ([]byte, int64, int64, []byte, error) {
	select {
	case <-it.interrupt:
		it.err = errCompactionAborted{}
		return nil, 0, 0, nil, it.err
	default:
	}

	blk := it.blocks[it.n][0]
	return blk.k, blk.minTime, blk.maxTime, blk.b, blk.err
}

func (it *cacheKeyIterator) Err() error {
	return it.err
}

func (it *cacheKeyIterator) Next() bool {
	if it.valid() {
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

func (it *cacheKeyIterator) EstimatedIndexSize() int {
	var n int
	for _, v := range it.nodes {
		n += len(v.GetKey())
	}
	return n
}

func (it *cacheKeyIterator) Close() error {
	it.c.Deref()
	return nil
}

func (it *cacheKeyIterator) valid() bool {
	return it.n != nil
}

func (it *cacheKeyIterator) encode() {
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
				key := curNode.GetKey()
				values := curNode.GetEntry().Values()

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

					if err != nil {
						it.err = err
					}
				}
				// Notify this key is fully encoded
				it.ready[curIdx] <- struct{}{}
			}
		}()
	}
}
