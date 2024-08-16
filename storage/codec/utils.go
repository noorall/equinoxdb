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

package codec

import (
	"encoding/binary"
	"equinox/storage/types"
	"fmt"
)

const (
	// BlockFloat64 designates a block encodes float64 values.
	BlockFloat64 = byte(0)

	// BlockInteger designates a block encodes int64 values.
	BlockInteger = byte(1)

	// BlockBoolean designates a block encodes boolean values.
	BlockBoolean = byte(2)

	// BlockString designates a block encodes string values.
	BlockString = byte(3)

	// BlockUnsigned designates a block encodes uint64 values.
	BlockUnsigned = byte(4)

	// BlockValuePtrMask we use the highest bit to determine whether it is Ptr
	BlockValuePtrMask = byte(0x80)

	// encodedBlockHeaderSize is the size of the header for an encoded block.  There is one
	// byte encoding the type of the block.
	encodedBlockHeaderSize = 1
)

func ZigZagEncode(x int64) uint64 {
	return uint64(x<<1) ^ uint64(x>>63)
}

func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

func BlockType(block []byte) (byte, error) {
	blockType := block[0] & ^BlockValuePtrMask
	switch blockType {
	case BlockFloat64, BlockInteger, BlockUnsigned, BlockBoolean, BlockString:
		return blockType, nil
	default:
		return 0, fmt.Errorf("unknown block type: %d", blockType)
	}
}

func GetBaseType(b byte) byte {
	return b & ^BlockValuePtrMask
}

func WithPtrFlag(b byte) byte {
	return b | BlockValuePtrMask
}

func IsPtrBlock(block []byte) bool {
	blockType := block[0]
	return (blockType & BlockValuePtrMask) != 0
}

func BlockCount(block []byte) (int, error) {
	if len(block) <= encodedBlockHeaderSize {
		return 0, fmt.Errorf("count of short block: got %v, exp %v", len(block), encodedBlockHeaderSize)
	}
	// first byte is the block type
	tb, _, err := unpackBlock(block[1:])
	if err != nil {
		return 0, fmt.Errorf("BlockCount: error unpacking block: %v", err)
	}
	// TODO: add implement of ptr
	return CountTimestamps(tb), nil
}

// DecodeBlock takes a byte slice and decodes it into values of the appropriate type
// based on the block.
func DecodeBlock(block []byte, vals []types.Value) ([]types.Value, error) {
	if len(block) <= encodedBlockHeaderSize {
		return nil, fmt.Errorf("decode of short block: got %v, exp %v", len(block), encodedBlockHeaderSize)
	}

	blockType, err := BlockType(block)
	if err != nil {
		return nil, fmt.Errorf("error decoding block type: %v", err)
	}

	switch blockType {
	case BlockFloat64:
		var buf []types.FloatValue
		decoded, err := DecodeFloatBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]types.Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err
	case BlockInteger:
		var buf []types.IntegerValue
		decoded, err := DecodeIntegerBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]types.Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockUnsigned:
		var buf []types.UnsignedValue
		decoded, err := DecodeUnsignedBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]types.Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockBoolean:
		var buf []types.BooleanValue
		decoded, err := DecodeBooleanBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]types.Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockString:
		var buf []types.StringValue
		decoded, err := DecodeStringBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]types.Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	default:
		return nil, fmt.Errorf("unknown block type: %d", blockType)
	}
}

func packBlock(buf []byte, typ byte, ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	sz := 1 + binary.MaxVarintLen64 + len(ts) + len(values)
	if cap(buf) < sz {
		buf = make([]byte, sz)
	}
	b := buf[:sz]
	b[0] = typ
	i := binary.PutUvarint(b[1:1+binary.MaxVarintLen64], uint64(len(ts)))
	i += 1

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	copy(b[i:], ts)
	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	copy(b[i+len(ts):], values)
	return b[:i+len(ts)+len(values)]
}

func unpackBlock(buf []byte) (ts, values []byte, err error) {
	// Unpack the timestamp block length
	tsLen, i := binary.Uvarint(buf)
	if i <= 0 {
		err = fmt.Errorf("unpackBlock: unable to read timestamp block length")
		return
	}

	// Unpack the timestamp bytes
	tsIdx := int(i) + int(tsLen)
	if tsIdx > len(buf) {
		err = fmt.Errorf("unpackBlock: not enough data for timestamp")
		return
	}
	ts = buf[int(i):tsIdx]

	// Unpack the value bytes
	values = buf[tsIdx:]
	return
}
