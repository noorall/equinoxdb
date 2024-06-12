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

// boolean encoding uses 1 bit per value.  Each compressed byte slice contains a 1 byte header
// indicating the compression type, followed by a variable byte encoded length indicating
// how many booleans are packed in the slice.  The remaining bytes contains 1 byte for every
// 8 boolean values encoded.

import (
	"encoding/binary"
	"equinox/storage/types"
	"fmt"
)

// Note: an uncompressed boolean format is not yet implemented.
// booleanCompressedBitPacked is a bit packed format using 1 bit per boolean
const booleanCompressedBitPacked = 1

// BooleanEncoder encodes a series of booleans to an in-memory buffer.
type BooleanEncoder struct {
	// The encoded bytes
	bytes []byte

	// The current byte being encoded
	b byte

	// The number of bools packed into b
	i int

	// The total number of bools written
	n int
}

// NewBooleanEncoder returns a new instance of BooleanEncoder.
func NewBooleanEncoder(sz int) BooleanEncoder {
	return BooleanEncoder{
		bytes: make([]byte, 0, (sz+7)/8),
	}
}

// Reset sets the encoder to its initial state.
func (e *BooleanEncoder) Reset() {
	e.bytes = e.bytes[:0]
	e.b = 0
	e.i = 0
	e.n = 0
}

// Write encodes b to the underlying buffer.
func (e *BooleanEncoder) Write(b bool) {
	// If we have filled the current byte, flush it
	if e.i >= 8 {
		e.flush()
	}

	// Use 1 bit for each boolean value, shift the current byte
	// by 1 and set the least significant bit accordingly
	e.b = e.b << 1
	if b {
		e.b |= 1
	}

	// Increment the current boolean count
	e.i++
	// Increment the total boolean count
	e.n++
}

func (e *BooleanEncoder) flush() {
	// Pad remaining byte w/ 0s
	for e.i < 8 {
		e.b = e.b << 1
		e.i++
	}

	// If we have bits set, append them to the byte slice
	if e.i > 0 {
		e.bytes = append(e.bytes, e.b)
		e.b = 0
		e.i = 0
	}
}

// Flush is no-op
func (e *BooleanEncoder) Flush() {}

// Bytes returns a new byte slice containing the encoded booleans from previous calls to Write.
func (e *BooleanEncoder) Bytes() ([]byte, error) {
	// Ensure the current byte is flushed
	e.flush()
	b := make([]byte, 10+1)

	// Store the encoding type in the 4 high bits of the first byte
	b[0] = byte(booleanCompressedBitPacked) << 4

	i := 1
	// Encode the number of booleans written
	i += binary.PutUvarint(b[i:], uint64(e.n))

	// Append the packed booleans
	return append(b[:i], e.bytes...), nil
}

// BooleanDecoder decodes a series of booleans from an in-memory buffer.
type BooleanDecoder struct {
	b   []byte
	i   int
	n   int
	err error
}

// SetBytes initializes the decoder with a new set of bytes to read from.
// This must be called before calling any other methods.
func (e *BooleanDecoder) SetBytes(b []byte) {
	if len(b) == 0 {
		return
	}

	// First byte stores the encoding type, only have 1 bit-packet format
	// currently ignore for now.
	b = b[1:]
	count, n := binary.Uvarint(b)
	if n <= 0 {
		e.err = fmt.Errorf("BooleanDecoder: invalid count")
		return
	}

	e.b = b[n:]
	e.i = -1
	e.n = int(count)

	if min := len(e.b) * 8; min < e.n {
		// Shouldn't happen - TSM file was truncated/corrupted
		e.n = min
	}
}

// Next returns whether there are any bits remaining in the decoder.
// It returns false if there was an error decoding.
// The error is available on the Error method.
func (e *BooleanDecoder) Next() bool {
	if e.err != nil {
		return false
	}

	e.i++
	return e.i < e.n
}

// Read returns the next bit from the decoder.
func (e *BooleanDecoder) Read() bool {
	// Index into the byte slice
	idx := e.i >> 3 // integer division by 8

	// Bit position
	pos := 7 - (e.i & 0x7)

	// The mask to select the bit
	mask := byte(1 << uint(pos))

	// The packed byte
	v := e.b[idx]

	// Returns true if the bit is set
	return v&mask == mask
}

// Error returns the error encountered during decoding, if one occurred.
func (e *BooleanDecoder) Error() error {
	return e.err
}

func EncodeBooleanBlock(buf []byte, values []types.Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A boolean block is encoded using different compression strategies
	// for timestamps and values.
	venc := GetBooleanEncoder(len(values))

	// Encode timestamps using an adaptive encoder
	tsenc := GetTimeEncoder(len(values))

	b, err := EncodeBooleanBlockUsing(buf, values, tsenc, venc)

	PutTimeEncoder(tsenc)
	PutBooleanEncoder(venc)

	return b, err
}

func EncodeBooleanBlockUsing(buf []byte, values []types.Value, tenc TimeEncoder, venc BooleanEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(types.BooleanValue)
		tenc.Write(vv.UnixNano())
		venc.Write(vv.RawValue())
	}

	// Encoded timestamp values
	tb, err := tenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded float values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	return packBlock(buf, BlockBoolean, tb, vb), nil
}

func DecodeBooleanBlock(block []byte, a *[]types.BooleanValue) ([]types.BooleanValue, error) {
	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockBoolean {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockBoolean, blockType)
	}
	block = block[1:]

	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	sz := CountTimestamps(tb)

	if cap(*a) < sz {
		*a = make([]types.BooleanValue, sz)
	} else {
		*a = (*a)[:sz]
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := booleanDecoderPool.Get(0).(*BooleanDecoder)

	var i int
	err = func(a []types.BooleanValue) error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		vdec.SetBytes(vb)

		// Decode both a timestamp and value
		j := 0
		for j < len(a) && tdec.Next() && vdec.Next() {
			a[j] = types.NewBooleanValue(tdec.Read(), vdec.Read()).(types.BooleanValue)
			j++
		}
		i = j

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did boolean decoding have an error?
		return vdec.Error()
	}(*a)

	timeDecoderPool.Put(tdec)
	booleanDecoderPool.Put(vdec)

	return (*a)[:i], err
}

func EncodeBooleanArrayBlock(a *types.BooleanArray, b []byte) ([]byte, error) {
	if a.Len() == 0 {
		return nil, nil
	}

	// TODO(edd): These need to be pooled.
	var vb []byte
	var tb []byte
	var err error

	if vb, err = BooleanArrayEncodeAll(a.Values, vb); err != nil {
		return nil, err
	}

	if tb, err = TimeArrayEncodeAll(a.Timestamps, tb); err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	return packBlock(b, BlockBoolean, tb, vb), nil
}

func DecodeBooleanArrayBlock(block []byte, a *types.BooleanArray) error {
	blockType := block[0]
	if blockType != BlockBoolean {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockBoolean, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = BooleanArrayDecodeAll(vb, a.Values)
	return err
}

// BooleanArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
func BooleanArrayEncodeAll(src []bool, b []byte) ([]byte, error) {
	sz := 1 + 8 + ((len(src) + 7) / 8) // Header + Num bools + bool data.
	if len(b) < sz && cap(b) > sz {
		b = b[:sz]
	} else if len(b) < sz {
		b = append(b, make([]byte, sz)...)
	}

	// Store the encoding type in the 4 high bits of the first byte
	b[0] = byte(booleanCompressedBitPacked) << 4
	n := uint64(8) // Current bit in current byte.

	// Encode the number of booleans written.
	i := binary.PutUvarint(b[n>>3:], uint64(len(src)))
	n += uint64(i * 8)

	for _, v := range src {
		if v {
			b[n>>3] |= 128 >> (n & 7) // Set current bit on current byte.
		} else {
			b[n>>3] &^= 128 >> (n & 7) // Clear current bit on current byte.
		}
		n++
	}

	length := n >> 3
	if n&7 > 0 {
		length++ // Add an extra byte to capture overflowing bits.
	}
	return b[:length], nil
}

func BooleanArrayDecodeAll(b []byte, dst []bool) ([]bool, error) {
	if len(b) == 0 {
		return nil, nil
	}

	// First byte stores the encoding type, only have 1 bit-packet format
	// currently ignore for now.
	b = b[1:]
	val, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, fmt.Errorf("BooleanBatchDecoder: invalid count")
	}

	count := int(val)

	b = b[n:]
	if min := len(b) * 8; min < count {
		// Shouldn't happen - TSM file was truncated/corrupted
		count = min
	}

	if cap(dst) < count {
		dst = make([]bool, count)
	} else {
		dst = dst[:count]
	}

	j := 0
	for _, v := range b {
		for i := byte(128); i > 0 && j < len(dst); i >>= 1 {
			dst[j] = v&i != 0
			j++
		}
	}
	return dst, nil
}
