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

// String encoding uses snappy compression to compress each string.  Each string is
// appended to byte slice prefixed with a variable byte length followed by the string
// bytes.  The bytes are compressed using snappy compressor and a 1 byte header is used
// to indicate the type of encoding.

import (
	"encoding/binary"
	"equinox/storage/types"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/golang/snappy"
)

// Note: an uncompressed format is not yet implemented.

// stringCompressedSnappy is a compressed encoding using Snappy compression
const stringCompressedSnappy = 1
const stringUnCompressedSnappy = 0

// StringEncoder encodes multiple strings into a byte slice.
type StringEncoder struct {
	// The encoded bytes
	bytes []byte
}

// NewStringEncoder returns a new StringEncoder with an initial buffer ready to hold sz bytes.
func NewStringEncoder(sz int) StringEncoder {
	return StringEncoder{
		bytes: make([]byte, 1, sz),
	}
}

// Flush is no-op
func (e *StringEncoder) Flush() {}

// Reset sets the encoder back to its initial state.
func (e *StringEncoder) Reset() {
	e.bytes[0] = 0
	e.bytes = e.bytes[:1]
}

// Write encodes s to the underlying buffer.
func (e *StringEncoder) Write(s string) {
	b := make([]byte, 10)
	// Append the length of the string using variable byte encoding
	i := binary.PutUvarint(b, uint64(len(s)))
	e.bytes = append(e.bytes, b[:i]...)

	// Append the string bytes
	e.bytes = append(e.bytes, s...)
}

// Bytes returns a copy of the underlying buffer.
func (e *StringEncoder) Bytes() ([]byte, error) {
	// Compress the currently appended bytes using snappy and prefix with
	// a 1 byte header for future extension
	data := snappy.Encode(nil, e.bytes[1:])
	return append([]byte{stringCompressedSnappy << 4}, data...), nil
}

// RawBytes returns a copy of the underlying buffer.
func (e *StringEncoder) RawBytes() ([]byte, error) {
	// Compress the currently appended bytes using snappy and prefix with
	// a 1 byte header for future extension
	e.bytes[0] = stringUnCompressedSnappy << 4
	return e.bytes, nil
}

// StringDecoder decodes a byte slice into strings.
type StringDecoder struct {
	b   []byte
	l   int
	i   int
	err error
}

// SetBytes initializes the decoder with bytes to read from.
// This must be called before calling any other method.
func (e *StringDecoder) SetBytes(b []byte) error {
	// First byte stores the encoding type, only have snappy format
	// currently so ignore for now.
	var data []byte
	if len(b) > 0 {
		var err error
		mask := b[0] >> 4
		if mask == stringCompressedSnappy {
			data, err = snappy.Decode(nil, b[1:])
			if err != nil {
				return fmt.Errorf("failed to decode string block: %v", err.Error())
			}
		} else {
			data = b[1:]
		}
	}

	e.b = data
	e.l = 0
	e.i = 0
	e.err = nil

	return nil
}

// Next returns true if there are any values remaining to be decoded.
func (e *StringDecoder) Next() bool {
	if e.err != nil {
		return false
	}

	e.i += e.l
	return e.i < len(e.b)
}

// Read returns the next value from the decoder.
func (e *StringDecoder) Read() string {
	// Read the length of the string
	length, n := binary.Uvarint(e.b[e.i:])
	if n <= 0 {
		e.err = fmt.Errorf("StringDecoder: invalid encoded string length")
		return ""
	}

	// The length of this string plus the length of the variable byte encoded length
	e.l = int(length) + n

	lower := e.i + n
	upper := lower + int(length)
	if upper < lower {
		e.err = fmt.Errorf("StringDecoder: length overflow")
		return ""
	}
	if upper > len(e.b) {
		e.err = fmt.Errorf("StringDecoder: not enough data to represent encoded string")
		return ""
	}

	return string(e.b[lower:upper])
}

// Error returns the last error encountered by the decoder.
func (e *StringDecoder) Error() error {
	return e.err
}

func EncodeStringBlock(buf []byte, values []types.Value) ([]byte, error) {
	tenc := GetTimeEncoder(len(values))
	venc := GetStringEncoder(len(values) * len(values[0].(types.StringValue).RawValue()))

	b, err := EncodeStringBlockUsing(buf, values, tenc, venc)

	PutTimeEncoder(tenc)
	PutStringEncoder(venc)

	return b, err
}

func EncodeStringBlockUsing(buf []byte, values []types.Value, tenc TimeEncoder, venc StringEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(types.StringValue)
		tenc.Write(vv.UnixNano())
		venc.Write(vv.RawValue())
	}

	// Encoded timestamp values
	tb, err := tenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded string values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	return packBlock(buf, BlockString, tb, vb), nil
}

func EncodeRawStringBlockUsing(buf []byte, values []types.Value, tenc TimeEncoder, venc StringEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(types.StringValue)
		tenc.Write(vv.UnixNano())
		venc.Write(vv.RawValue())
	}

	// Encoded timestamp values
	tb, err := tenc.RawBytes()
	if err != nil {
		return nil, err
	}
	// Encoded string values
	vb, err := venc.RawBytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	return packBlock(buf, BlockString, tb, vb), nil
}

func DecodeStringBlock(block []byte, a *[]types.StringValue) ([]types.StringValue, error) {
	blockType := block[0]
	if blockType != BlockString {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockString, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb, err := unpackBlock(block)
	if err != nil {
		return nil, err
	}

	sz := CountTimestamps(tb)

	if cap(*a) < sz {
		*a = make([]types.StringValue, sz)
	} else {
		*a = (*a)[:sz]
	}

	tdec := timeDecoderPool.Get(0).(*TimeDecoder)
	vdec := stringDecoderPool.Get(0).(*StringDecoder)

	var i int
	err = func(a []types.StringValue) error {
		// Setup our timestamp and value decoders
		tdec.Init(tb)
		err = vdec.SetBytes(vb)
		if err != nil {
			return err
		}

		// Decode both a timestamp and value
		j := 0
		for j < len(a) && tdec.Next() && vdec.Next() {
			a[j] = types.NewStringValue(tdec.Read(), vdec.Read()).(types.StringValue)
			j++
		}
		i = j

		// Did timestamp decoding have an error?
		err = tdec.Error()
		if err != nil {
			return err
		}
		// Did string decoding have an error?
		return vdec.Error()
	}(*a)

	timeDecoderPool.Put(tdec)
	stringDecoderPool.Put(vdec)

	return (*a)[:i], err
}

func EncodeStringArrayBlock(a *types.StringArray, b []byte) ([]byte, error) {
	if a.Len() == 0 {
		return nil, nil
	}

	// TODO(edd): These need to be pooled.
	var vb []byte
	var tb []byte
	var err error

	if vb, err = StringArrayEncodeAll(a.Values, vb); err != nil {
		return nil, err
	}

	if tb, err = TimeArrayEncodeAll(a.Timestamps, tb); err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	return packBlock(b, BlockString, tb, vb), nil
}

func DecodeStringArrayBlock(block []byte, a *types.StringArray) error {
	blockType := block[0]
	if blockType != BlockString {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockString, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = StringArrayDecodeAll(vb, a.Values)
	return err
}

var (
	errStringBatchDecodeInvalidStringLength = fmt.Errorf("StringArrayDecodeAll: invalid encoded string length")
	errStringBatchDecodeLengthOverflow      = fmt.Errorf("StringArrayDecodeAll: length overflow")
	errStringBatchDecodeShortBuffer         = fmt.Errorf("StringArrayDecodeAll: short buffer")

	// ErrStringArrayEncodeTooLarge reports that the encoded length of a slice of strings is too large.
	ErrStringArrayEncodeTooLarge = errors.New("StringArrayEncodeAll: source length too large")
)

// StringArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// Currently only the string compression scheme used snappy.
func StringArrayEncodeAll(src []string, b []byte) ([]byte, error) {
	srcSz64 := int64(2 + len(src)*binary.MaxVarintLen32) // strings shouldn't be longer than 64kb
	for i := range src {
		srcSz64 += int64(len(src[i]))
	}

	// 32-bit systems
	if srcSz64 > math.MaxUint32 {
		return b[:0], ErrStringArrayEncodeTooLarge
	}

	srcSz := int(srcSz64)

	// determine the maximum possible length needed for the buffer, which
	// includes the compressed size
	var compressedSz = 0
	if len(src) > 0 {
		mle := snappy.MaxEncodedLen(srcSz)
		if mle == -1 {
			return b[:0], ErrStringArrayEncodeTooLarge
		}
		compressedSz = mle + 1 /* header */
	}
	totSz := srcSz + compressedSz

	if cap(b) < totSz {
		b = make([]byte, totSz)
	} else {
		b = b[:totSz]
	}

	// Shortcut to snappy encoding nothing.
	if len(src) == 0 {
		b[0] = stringCompressedSnappy << 4
		return b[:2], nil
	}

	// write the data to be compressed *after* the space needed for snappy
	// compression. The compressed data is at the start of the allocated buffer,
	// ensuring the entire capacity is returned and available for subsequent use.
	dta := b[compressedSz:]
	n := 0
	for i := range src {
		n += binary.PutUvarint(dta[n:], uint64(len(src[i])))
		n += copy(dta[n:], src[i])
	}
	dta = dta[:n]

	dst := b[:compressedSz]
	dst[0] = stringCompressedSnappy << 4
	res := snappy.Encode(dst[1:], dta)
	return dst[:len(res)+1], nil
}

func StringArrayDecodeAll(b []byte, dst []string) ([]string, error) {
	// First byte stores the encoding type, only have snappy format
	// currently so ignore for now.
	if len(b) > 0 {
		var err error
		// it is important that to note that `snappy.Decode` always returns
		// a newly allocated slice as the final strings reference this slice
		// directly.
		mask := b[0] >> 4
		if mask == stringCompressedSnappy {
			b, err = snappy.Decode(nil, b[1:])
			if err != nil {
				return []string{}, fmt.Errorf("failed to decode string block: %v", err.Error())
			}
		} else {
			b = b[1:]
		}

	} else {
		return []string{}, nil
	}

	var (
		i, l int
	)

	sz := cap(dst)
	if sz == 0 {
		sz = 64
		dst = make([]string, sz)
	} else {
		dst = dst[:sz]
	}

	j := 0

	for i < len(b) {
		length, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return []string{}, errStringBatchDecodeInvalidStringLength
		}

		// The length of this string plus the length of the variable byte encoded length
		l = int(length) + n

		lower := i + n
		upper := lower + int(length)
		if upper < lower {
			return []string{}, errStringBatchDecodeLengthOverflow
		}
		if upper > len(b) {
			return []string{}, errStringBatchDecodeShortBuffer
		}

		// NOTE: this optimization is critical for performance and to reduce
		// allocations. This is just as "safe" as string.Builder, which
		// returns a string mapped to the original byte slice
		s := b[lower:upper]
		val := *(*string)(unsafe.Pointer(&s))
		if j < len(dst) {
			dst[j] = val
		} else {
			dst = append(dst, val) // force a resize
			dst = dst[:cap(dst)]
		}
		i += l
		j++
	}

	return dst[:j], nil
}
