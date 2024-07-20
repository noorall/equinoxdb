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

import (
	"bytes"
	"equinox/storage/store"
	"equinox/storage/types"
	"fmt"
	"math"
)

// tsmBatchKeyIterator implements the KeyIterator for set of store.TSMReaders.  Iteration produces
// keys in sorted order and the values between the keys sorted and deduped.  If any of
// the readers have associated tombstone entries, they are returned as part of iteration.
type tsmBatchKeyIterator struct {
	// readers is the set of readers it produce a sorted key run with
	readers []*store.TSMReader

	// values is the temporary buffers for each key that is returned by a reader
	values map[string][]types.Value

	// pos is the current key position within the corresponding readers slice.  A value of
	// pos[0] = 1, means the reader[0] is currently at key 1 in its ordered index.
	pos []int

	// errs is any error we received while iterating values.
	errs TSMErrors

	// indicates whether the iterator should choose a faster merging strategy over a more
	// optimally compressed one.  If fast is true, multiple blocks will just be added as is
	// and not combined.  In some cases, a slower path will need to be utilized even when
	// fast is true to prevent overlapping blocks of time for the same key.
	// If false, the blocks will be decoded and duplicated (if needed) and
	// then chunked into the maximally sized blocks.
	fast bool

	// size is the maximum number of values to encode in a single block
	size int

	// key is the current key lowest key across all readers that has not be fully exhausted
	// of values.
	key []byte
	typ byte

	// tsmFiles are the string names of the files for use in tracking errors, ordered the same
	// as iterators and buf
	tsmFiles []string
	// currentTsm is the current TSM File being iterated over
	currentTsm string

	iterators []*store.BlockIterator
	blocks    blocks

	buf []blocks

	// mergeValues are decoded blocks that have been combined
	mergedFloatValues    *types.FloatArray
	mergedIntegerValues  *types.IntegerArray
	mergedUnsignedValues *types.UnsignedArray
	mergedBooleanValues  *types.BooleanArray
	mergedStringValues   *types.StringArray

	// merged are encoded blocks that have been combined or used as is
	// without decode
	merged    blocks
	interrupt chan struct{}

	// maxErrors is the maximum number of errors to store before discarding.
	maxErrors int
	// overflowErrors is the number of errors we have ignored.
	overflowErrors int
}

func (it *tsmBatchKeyIterator) AppendError(err error) bool {
	if it.maxErrors > len(it.errs) {
		it.errs = append(it.errs, err)
		// Was the error stored?
		return true
	} else {
		// Was the error dropped
		it.overflowErrors++
		return false
	}
}

// NewTSMBatchKeyIterator returns a new TSM key iterator from readers.
// size indicates the maximum number of values to encode in a single block.
func NewTSMBatchKeyIterator(size int, fast bool, maxErrors int, interrupt chan struct{}, tsmFiles []string, readers ...*store.TSMReader) (KeyIterator, error) {
	var iter []*store.BlockIterator
	for _, r := range readers {
		iter = append(iter, r.BlockIterator())
	}

	return &tsmBatchKeyIterator{
		readers:              readers,
		values:               map[string][]types.Value{},
		pos:                  make([]int, len(readers)),
		size:                 size,
		iterators:            iter,
		fast:                 fast,
		tsmFiles:             tsmFiles,
		buf:                  make([]blocks, len(iter)),
		mergedFloatValues:    &types.FloatArray{},
		mergedIntegerValues:  &types.IntegerArray{},
		mergedUnsignedValues: &types.UnsignedArray{},
		mergedBooleanValues:  &types.BooleanArray{},
		mergedStringValues:   &types.StringArray{},
		interrupt:            interrupt,
		maxErrors:            maxErrors,
	}, nil
}

func (it *tsmBatchKeyIterator) hasMergedValues() bool {
	return it.mergedFloatValues.Len() > 0 ||
		it.mergedIntegerValues.Len() > 0 ||
		it.mergedUnsignedValues.Len() > 0 ||
		it.mergedStringValues.Len() > 0 ||
		it.mergedBooleanValues.Len() > 0
}

func (it *tsmBatchKeyIterator) EstimatedIndexSize() int {
	var size uint32
	for _, r := range it.readers {
		size += r.IndexSize()
	}
	return int(size) / len(it.readers)
}

// Next returns true if there are any values remaining in the iterator.
func (it *tsmBatchKeyIterator) Next() bool {
RETRY:
	// Any merged blocks pending?
	if len(it.merged) > 0 {
		it.merged = it.merged[1:]
		if len(it.merged) > 0 {
			return true
		}
	}

	// Any merged values pending?
	if it.hasMergedValues() {
		it.merge()
		if len(it.merged) > 0 || it.hasMergedValues() {
			return true
		}
	}

	// If we still have blocks from the last read, merge them
	if len(it.blocks) > 0 {
		it.merge()
		if len(it.merged) > 0 || it.hasMergedValues() {
			return true
		}
	}

	// Read the next block from each TSM iterator
	for i, v := range it.buf {
		if len(v) != 0 {
			continue
		}

		iter := it.iterators[i]
		it.currentTsm = it.tsmFiles[i]
		if iter.Next() {
			key, minTime, maxTime, typ, _, b, err := iter.Read()
			if err != nil {
				it.AppendError(ErrBlockRead{it.currentTsm, err})
			}

			// This block may have ranges of time removed from it that would
			// reduce the block min and max time.
			tombstones := iter.R.TombstoneRange(key)

			var blk *block
			if cap(it.buf[i]) > len(it.buf[i]) {
				it.buf[i] = it.buf[i][:len(it.buf[i])+1]
				blk = it.buf[i][len(it.buf[i])-1]
				if blk == nil {
					blk = &block{}
					it.buf[i][len(it.buf[i])-1] = blk
				}
			} else {
				blk = &block{}
				it.buf[i] = append(it.buf[i], blk)
			}
			blk.minTime = minTime
			blk.maxTime = maxTime
			blk.key = key
			blk.typ = typ
			blk.b = b
			blk.tombstones = tombstones
			blk.readMin = math.MaxInt64
			blk.readMax = math.MinInt64

			blockKey := key
			for bytes.Equal(iter.PeekNext(), blockKey) {
				iter.Next()
				key, minTime, maxTime, typ, _, b, err := iter.Read()
				if err != nil {
					it.AppendError(ErrBlockRead{it.currentTsm, err})
				}

				tombstones := iter.R.TombstoneRange(key)

				var blk *block
				if cap(it.buf[i]) > len(it.buf[i]) {
					it.buf[i] = it.buf[i][:len(it.buf[i])+1]
					blk = it.buf[i][len(it.buf[i])-1]
					if blk == nil {
						blk = &block{}
						it.buf[i][len(it.buf[i])-1] = blk
					}
				} else {
					blk = &block{}
					it.buf[i] = append(it.buf[i], blk)
				}

				blk.minTime = minTime
				blk.maxTime = maxTime
				blk.key = key
				blk.typ = typ
				blk.b = b
				blk.tombstones = tombstones
				blk.readMin = math.MaxInt64
				blk.readMax = math.MinInt64
			}
		}

		if iter.Err() != nil {
			it.AppendError(ErrBlockRead{it.currentTsm, iter.Err()})
		}
	}

	// Each reader could have a different key that it's currently at, need to find
	// the next smallest one to keep the sort ordering.
	var minKey []byte
	var minType byte
	for _, b := range it.buf {
		// block could be nil if the iterator has been exhausted for that File
		if len(b) == 0 {
			continue
		}
		if len(minKey) == 0 || bytes.Compare(b[0].key, minKey) < 0 {
			minKey = b[0].key
			minType = b[0].typ
		}
	}
	it.key = minKey
	it.typ = minType

	// Now we need to find all blocks that match the min key so we can combine and dedupe
	// the blocks if necessary
	for i, b := range it.buf {
		if len(b) == 0 {
			continue
		}
		if bytes.Equal(b[0].key, it.key) {
			it.blocks = append(it.blocks, b...)
			it.buf[i] = it.buf[i][:0]
		}
	}

	if len(it.blocks) == 0 {
		return false
	}

	it.merge()

	// After merging all the values for this key, we might not have any.  (e.g. they were all deleted
	// through many tombstones).  In this case, move on to the next key instead of ending iteration.
	if len(it.merged) == 0 {
		goto RETRY
	}

	return len(it.merged) > 0
}

// merge combines the next set of blocks into merged blocks.
func (it *tsmBatchKeyIterator) merge() {
	switch it.typ {
	case types.BlockFloat64:
		it.mergeFloat()
	case types.BlockInteger:
		it.mergeInteger()
	case types.BlockUnsigned:
		it.mergeUnsigned()
	case types.BlockBoolean:
		it.mergeBoolean()
	case types.BlockString:
		it.mergeString()
	default:
		it.AppendError(ErrBlockRead{it.currentTsm, fmt.Errorf("unknown block type: %v", it.typ)})
	}
}

func (it *tsmBatchKeyIterator) handleEncodeError(err error, typ string) {
	it.AppendError(ErrBlockRead{it.currentTsm, fmt.Errorf("encode error: unable to compress block type %s for key '%s': %v", typ, it.key, err)})
}

func (it *tsmBatchKeyIterator) handleDecodeError(err error, typ string) {
	it.AppendError(ErrBlockRead{it.currentTsm, fmt.Errorf("decode error: unable to decompress block type %s for key '%s': %v", typ, it.key, err)})
}

func (it *tsmBatchKeyIterator) Read() ([]byte, int64, int64, []byte, error) {
	// See if compactions were disabled while we were running.
	select {
	case <-it.interrupt:
		return nil, 0, 0, nil, ErrCompactionAborted{}
	default:
	}

	if len(it.merged) == 0 {
		return nil, 0, 0, nil, it.Err()
	}

	block := it.merged[0]
	return block.key, block.minTime, block.maxTime, block.b, it.Err()
}

func (it *tsmBatchKeyIterator) Close() error {
	it.values = nil
	it.pos = nil
	it.iterators = nil
	for _, r := range it.readers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Err returns any errors encountered during iteration.
func (it *tsmBatchKeyIterator) Err() error {
	if len(it.errs) == 0 {
		return nil
	}
	// Copy the errors before appending the dropped error count
	var errs TSMErrors
	errs = make([]error, 0, len(it.errs)+1)
	errs = append(errs, it.errs...)
	errs = append(errs, fmt.Errorf("additional errors dropped: %d", it.overflowErrors))
	return errs
}
