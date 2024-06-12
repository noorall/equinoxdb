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

// Integer encoding uses two different strategies depending on the range of values in
// the uncompressed data.  Encoded values are first encoding used zig zag encoding.
// This interleaves positive and negative integers across a range of positive integers.
//
// For example, [-2,-1,0,1] becomes [3,1,0,2]. See
// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
// for more information.
//
// If all the zig zag encoded values are less than 1 << 60 - 1, they are compressed using
// simple8b encoding.  If any value is larger than 1 << 60 - 1, the values are stored uncompressed.
//
// Each encoded byte slice contains a 1 byte header followed by multiple 8 byte packed integers
// or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
// for the remaining bytes.
//
// There are currently two encoding types that can be used with room for 16 total.  These additional
// encoding slots are reserved for future use.  One improvement to be made is to use a patched
// encoding such as PFOR if only a small number of values exceed the max compressed value range.  This
// should improve compression ratios with very large integers near the ends of the int64 range.

import (
	"encoding/binary"
	"equinox/pkg/encoding/simple8b"
	"equinox/storage/types"
	"fmt"
	"unsafe"
)

const (
	// intUncompressed is an uncompressed format using 8 bytes per point
	intUncompressed = 0
	// intCompressedSimple is a bit-packed format using simple8b encoding
	intCompressedSimple = 1
	// intCompressedRLE is a run-length encoding format
	intCompressedRLE = 2
)

// IntegerEncoder encodes int64s into byte slices.
type IntegerEncoder struct {
	prev   int64
	rle    bool
	values []uint64
}

// NewIntegerEncoder returns a new integer encoder with an initial buffer of values sized at sz.
func NewIntegerEncoder(sz int) IntegerEncoder {
	return IntegerEncoder{
		rle:    true,
		values: make([]uint64, 0, sz),
	}
}

// Flush is no-op
func (e *IntegerEncoder) Flush() {}

// Reset sets the encoder back to its initial state.
func (e *IntegerEncoder) Reset() {
	e.prev = 0
	e.rle = true
	e.values = e.values[:0]
}

// Write encodes v to the underlying buffers.
func (e *IntegerEncoder) Write(v int64) {
	// Delta-encode each value as it's written.  This happens before
	// ZigZagEncoding because the deltas could be negative.
	delta := v - e.prev
	e.prev = v
	enc := ZigZagEncode(delta)
	if len(e.values) > 1 {
		e.rle = e.rle && e.values[len(e.values)-1] == enc
	}

	e.values = append(e.values, enc)
}

// Bytes returns a copy of the underlying buffer.
func (e *IntegerEncoder) Bytes() ([]byte, error) {
	// Only run-length encode if it could reduce storage size.
	if e.rle && len(e.values) > 2 {
		return e.encodeRLE()
	}

	for _, v := range e.values {
		// Value is too large to encode using packed format
		if v > simple8b.MaxValue {
			return e.encodeUncompressed()
		}
	}

	return e.encodePacked()
}

func (e *IntegerEncoder) encodeRLE() ([]byte, error) {
	// Large varints can take up to 10 bytes.  We're storing 3 + 1
	// type byte.
	var b [31]byte

	// 4 high bits used for the encoding type
	b[0] = byte(intCompressedRLE) << 4

	i := 1
	// The first value
	binary.BigEndian.PutUint64(b[i:], e.values[0])
	i += 8
	// The first delta
	i += binary.PutUvarint(b[i:], e.values[1])
	// The number of times the delta is repeated
	i += binary.PutUvarint(b[i:], uint64(len(e.values)-1))

	return b[:i], nil
}

func (e *IntegerEncoder) encodePacked() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	// Encode all but the first value.  Fist value is written unencoded
	// using 8 bytes.
	encoded, err := simple8b.EncodeAll(e.values[1:])
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1+(len(encoded)+1)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intCompressedSimple) << 4

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], e.values[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b, nil
}

func (e *IntegerEncoder) encodeUncompressed() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	b := make([]byte, 1+len(e.values)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intUncompressed) << 4

	for i, v := range e.values {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
	}
	return b, nil
}

// IntegerDecoder decodes a byte slice into int64s.
type IntegerDecoder struct {
	// 240 is the maximum number of values that can be encoded into a single uint64 using simple8b
	values [240]uint64
	bytes  []byte
	i      int
	n      int
	prev   int64
	first  bool

	// The first value for a run-length encoded byte slice
	rleFirst uint64

	// The delta value for a run-length encoded byte slice
	rleDelta uint64
	encoding byte
	err      error
}

// SetBytes sets the underlying byte slice of the decoder.
func (d *IntegerDecoder) SetBytes(b []byte) {
	if len(b) > 0 {
		d.encoding = b[0] >> 4
		d.bytes = b[1:]
	} else {
		d.encoding = 0
		d.bytes = nil
	}

	d.i = 0
	d.n = 0
	d.prev = 0
	d.first = true

	d.rleFirst = 0
	d.rleDelta = 0
	d.err = nil
}

// Next returns true if there are any values remaining to be decoded.
func (d *IntegerDecoder) Next() bool {
	if d.i >= d.n && len(d.bytes) == 0 {
		return false
	}

	d.i++

	if d.i >= d.n {
		switch d.encoding {
		case intUncompressed:
			d.decodeUncompressed()
		case intCompressedSimple:
			d.decodePacked()
		case intCompressedRLE:
			d.decodeRLE()
		default:
			d.err = fmt.Errorf("unknown encoding %v", d.encoding)
		}
	}
	return d.err == nil && d.i < d.n
}

// Error returns the last error encountered by the decoder.
func (d *IntegerDecoder) Error() error {
	return d.err
}

// Read returns the next value from the decoder.
func (d *IntegerDecoder) Read() int64 {
	switch d.encoding {
	case intCompressedRLE:
		return ZigZagDecode(d.rleFirst) + int64(d.i)*ZigZagDecode(d.rleDelta)
	default:
		v := ZigZagDecode(d.values[d.i])
		// v is the delta encoded value, we need to add the prior value to get the original
		v = v + d.prev
		d.prev = v
		return v
	}
}

func (d *IntegerDecoder) decodeRLE() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode RLE starting value")
		return
	}

	var i, n int

	// Next 8 bytes is the starting value
	first := binary.BigEndian.Uint64(d.bytes[i : i+8])
	i += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(d.bytes[i:])
	if n <= 0 {
		d.err = fmt.Errorf("IntegerDecoder: invalid RLE delta value")
		return
	}
	i += n

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(d.bytes[i:])
	if n <= 0 {
		d.err = fmt.Errorf("IntegerDecoder: invalid RLE repeat value")
		return
	}

	// Store the first value and delta value so we do not need to allocate
	// a large values slice.  We can compute the value at position d.i on
	// demand.
	d.rleFirst = first
	d.rleDelta = value
	d.n = int(count) + 1
	d.i = 0

	// We've process all the bytes
	d.bytes = nil
}

func (d *IntegerDecoder) decodePacked() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode packed value")
		return
	}

	v := binary.BigEndian.Uint64(d.bytes[0:8])
	// The first value is always unencoded
	if d.first {
		d.first = false
		d.n = 1
		d.values[0] = v
	} else {
		n, err := simple8b.Decode(&d.values, v)
		if err != nil {
			// Should never happen, only error that could be returned is if the value to be decoded was not
			// actually encoded by simple8b encoder.
			d.err = fmt.Errorf("failed to decode value %v: %v", v, err)
		}

		d.n = n
	}
	d.i = 0
	d.bytes = d.bytes[8:]
}

func (d *IntegerDecoder) decodeUncompressed() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode uncompressed value")
		return
	}

	d.values[0] = binary.BigEndian.Uint64(d.bytes[0:8])
	d.i = 0
	d.n = 1
	d.bytes = d.bytes[8:]
}

func EncodeIntegerBlock(buf []byte, values []types.Value) ([]byte, error) {
	tenc := GetTimeEncoder(len(values))
	venc := GetIntegerEncoder(len(values))

	b, err := EncodeIntegerBlockUsing(buf, values, tenc, venc)

	PutTimeEncoder(tenc)
	PutIntegerEncoder(venc)

	return b, err
}

func EncodeIntegerBlockUsing(buf []byte, values []types.Value, tenc TimeEncoder, venc IntegerEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(types.IntegerValue)
		tenc.Write(vv.UnixNano())
		venc.Write(vv.RawValue())
	}

	// Encoded timestamp values
	tb, err := tenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded int64 values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	return packBlock(buf, BlockInteger, tb, vb), nil
}

func DecodeIntegerBlock(block []byte, a *[]types.IntegerValue) ([]types.IntegerValue, error) {
	blockType := block[0]
	if blockType != BlockInteger {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockInteger, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	sz := CountTimestamps(tb)

	if cap(*a) < sz {
		*a = make([]types.IntegerValue, sz)
	} else {
		*a = (*a)[:sz]
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := integerDecoderPool.Get(0).(*IntegerDecoder)

	var i int
	err = func(a []types.IntegerValue) error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		vdec.SetBytes(vb)

		// Decode both a timestamp and value
		j := 0
		for j < len(a) && tdec.Next() && vdec.Next() {
			a[j] = types.NewIntegerValue(tdec.Read(), vdec.Read()).(types.IntegerValue)
			j++
		}
		i = j

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did int64 decoding have an error?
		return vdec.Error()
	}(*a)

	timeDecoderPool.Put(tdec)
	integerDecoderPool.Put(vdec)

	return (*a)[:i], err
}

func DecodeUnsignedBlock(block []byte, a *[]types.UnsignedValue) ([]types.UnsignedValue, error) {
	blockType := block[0]
	if blockType != BlockUnsigned {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockUnsigned, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	sz := CountTimestamps(tb)

	if cap(*a) < sz {
		*a = make([]types.UnsignedValue, sz)
	} else {
		*a = (*a)[:sz]
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := integerDecoderPool.Get(0).(*IntegerDecoder)

	var i int
	err = func(a []types.UnsignedValue) error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		vdec.SetBytes(vb)

		// Decode both a timestamp and value
		j := 0
		for j < len(a) && tdec.Next() && vdec.Next() {
			a[j] = types.NewUnsignedValue(tdec.Read(), uint64(vdec.Read())).(types.UnsignedValue)
			j++
		}
		i = j

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did int64 decoding have an error?
		return vdec.Error()
	}(*a)

	timeDecoderPool.Put(tdec)
	integerDecoderPool.Put(vdec)

	return (*a)[:i], err
}

func EncodeUnsignedBlock(buf []byte, values []types.Value) ([]byte, error) {
	tenc := GetTimeEncoder(len(values))
	venc := GetUnsignedEncoder(len(values))

	b, err := EncodeUnsignedBlockUsing(buf, values, tenc, venc)

	PutTimeEncoder(tenc)
	PutUnsignedEncoder(venc)

	return b, err
}

func EncodeUnsignedBlockUsing(buf []byte, values []types.Value, tenc TimeEncoder, venc IntegerEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(types.UnsignedValue)
		tenc.Write(vv.UnixNano())
		venc.Write(int64(vv.RawValue()))
	}

	// Encoded timestamp values
	tb, err := tenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded int64 values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	return packBlock(buf, BlockUnsigned, tb, vb), nil
}

func EncodeIntegerArrayBlock(a *types.IntegerArray, b []byte) ([]byte, error) {
	if a.Len() == 0 {
		return nil, nil
	}

	// TODO(edd): These need to be pooled.
	var vb []byte
	var tb []byte
	var err error

	if vb, err = IntegerArrayEncodeAll(a.Values, vb); err != nil {
		return nil, err
	}

	if tb, err = TimeArrayEncodeAll(a.Timestamps, tb); err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	return packBlock(b, BlockInteger, tb, vb), nil
}

func DecodeIntegerArrayBlock(block []byte, a *types.IntegerArray) error {
	blockType := block[0]
	if blockType != BlockInteger {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockInteger, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = IntegerArrayDecodeAll(vb, a.Values)
	return err
}

func EncodeUnsignedArrayBlock(a *types.UnsignedArray, b []byte) ([]byte, error) {
	if a.Len() == 0 {
		return nil, nil
	}

	// TODO(edd): These need to be pooled.
	var vb []byte
	var tb []byte
	var err error

	if vb, err = UnsignedArrayEncodeAll(a.Values, vb); err != nil {
		return nil, err
	}

	if tb, err = TimeArrayEncodeAll(a.Timestamps, tb); err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	return packBlock(b, BlockUnsigned, tb, vb), nil
}

func DecodeUnsignedArrayBlock(block []byte, a *types.UnsignedArray) error {
	blockType := block[0]
	if blockType != BlockUnsigned {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockUnsigned, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = UnsignedArrayDecodeAll(vb, a.Values)
	return err
}

// IntegerArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// IntegerArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: IntegerArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into IntegerArrayEncodeAll.
func IntegerArrayEncodeAll(src []int64, b []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil // Nothing to do
	}

	var max = uint64(0)

	// To prevent an allocation of the entire block we're encoding reuse the
	// src slice to store the encoded deltas.
	deltas := reinterpretInt64ToUint64Slice(src)
	for i := len(deltas) - 1; i > 0; i-- {
		deltas[i] = deltas[i] - deltas[i-1]
		deltas[i] = ZigZagEncode(int64(deltas[i]))
		if deltas[i] > max {
			max = deltas[i]
		}
	}

	deltas[0] = ZigZagEncode(int64(deltas[0]))

	if len(deltas) > 2 {
		var rle = true
		for i := 2; i < len(deltas); i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		if rle {
			// Large varints can take up to 10 bytes.  We're storing 3 + 1
			// type byte.
			if len(b) < 31 && cap(b) >= 31 {
				b = b[:31]
			} else if len(b) < 31 {
				b = append(b, make([]byte, 31-len(b))...)
			}

			// 4 high bits used for the encoding type
			b[0] = byte(intCompressedRLE) << 4

			i := 1
			// The first value
			binary.BigEndian.PutUint64(b[i:], deltas[0])
			i += 8
			// The first delta
			i += binary.PutUvarint(b[i:], deltas[1])
			// The number of times the delta is repeated
			i += binary.PutUvarint(b[i:], uint64(len(deltas)-1))

			return b[:i], nil
		}
	}

	if max > simple8b.MaxValue { // There is an encoded value that's too big to simple8b encode.
		// Encode uncompressed.
		sz := 1 + len(deltas)*8
		if len(b) < sz && cap(b) >= sz {
			b = b[:sz]
		} else if len(b) < sz {
			b = append(b, make([]byte, sz-len(b))...)
		}

		// 4 high bits of first byte store the encoding type for the block
		b[0] = byte(intUncompressed) << 4
		for i, v := range deltas {
			binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
		}
		return b[:sz], nil
	}

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := simple8b.EncodeAll(deltas[1:])
	if err != nil {
		return nil, err
	}

	sz := 1 + (len(encoded)+1)*8
	if len(b) < sz && cap(b) >= sz {
		b = b[:sz]
	} else if len(b) < sz {
		b = append(b, make([]byte, sz-len(b))...)
	}

	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intCompressedSimple) << 4

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], deltas[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b, nil
}

// UnsignedArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// UnsignedArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: IntegerArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into IntegerArrayEncodeAll.
func UnsignedArrayEncodeAll(src []uint64, b []byte) ([]byte, error) {
	srcint := reinterpretUint64ToInt64Slice(src)
	return IntegerArrayEncodeAll(srcint, b)
}

var (
	integerBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		integerBatchDecodeAllUncompressed,
		integerBatchDecodeAllSimple,
		integerBatchDecodeAllRLE,
		integerBatchDecodeAllInvalid,
	}
)

func IntegerArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return integerBatchDecoderFunc[encoding&3](b, dst)
}

func UnsignedArrayDecodeAll(b []byte, dst []uint64) ([]uint64, error) {
	if len(b) == 0 {
		return []uint64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := integerBatchDecoderFunc[encoding&3](b, reinterpretUint64ToInt64Slice(dst))
	return reinterpretInt64ToUint64Slice(res), err
}

func integerBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := int64(0)
	for i := range dst {
		prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: not enough data to decode packed value")
	}

	count, err := simple8b.CountBytes(b[8:])
	if err != nil {
		return []int64{}, err
	}

	count += 1
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	// first value
	dst[0] = ZigZagDecode(binary.BigEndian.Uint64(b))

	// decode compressed values
	buf := reinterpretInt64ToUint64Slice(dst)
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[8:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// calculate prefix sum
	prev := dst[0]
	for i := 1; i < len(dst); i++ {
		prev += ZigZagDecode(uint64(dst[i]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: invalid RLE delta value")
	}
	k += n

	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerArrayDecodeAll: invalid RLE repeat value")
	}

	count += 1

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	if delta == 0 {
		for i := range dst {
			dst[i] = first
		}
	} else {
		acc := first
		for i := range dst {
			dst[i] = acc
			acc += delta
		}
	}

	return dst, nil
}

func integerBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("unknown encoding %v", b[0]>>4)
}

func reinterpretInt64ToUint64Slice(src []int64) []uint64 {
	return *(*[]uint64)(unsafe.Pointer(&src))
}

func reinterpretUint64ToInt64Slice(src []uint64) []int64 {
	return *(*[]int64)(unsafe.Pointer(&src))
}
