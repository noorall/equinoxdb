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
	"equinox/pkg/encoding/simple8b"
	"fmt"
	"math"
	"unsafe"
)

const (
	// timeUncompressed is an uncompressed format using 8 bytes per timestamp
	timeUncompressed = 0
	// timeCompressedPackedSimple is a bit-packed format using simple8b encoding
	timeCompressedPackedSimple = 1
	// timeCompressedRLE is a run-length encoding format
	timeCompressedRLE = 2
)

type TimeEncoder interface {
	Write(t int64)
	Bytes() ([]byte, error)
	Reset()
	RawBytes() ([]byte, error)
}

type encoder struct {
	ts    []uint64
	bytes []byte
	enc   *simple8b.Encoder
}

// NewTimeEncoder returns a TimeEncoder with an initial buffer ready to hold sz bytes.
func NewTimeEncoder(sz int) TimeEncoder {
	return &encoder{
		ts:  make([]uint64, 0, sz),
		enc: simple8b.NewEncoder(),
	}
}

// Reset sets the encoding back to its initial state.
func (e *encoder) Reset() {
	e.ts = e.ts[:0]
	e.bytes = e.bytes[:0]
	e.enc.Reset()
}

// Write adds a timestamp to the compressed stream.
func (e *encoder) Write(t int64) {
	e.ts = append(e.ts, uint64(t))
}

func (e *encoder) reduce() (max, divisor uint64, rle bool, deltas []uint64) {
	// Compute the deltas in place to avoid allocating another slice
	deltas = e.ts
	// Starting values for a max and divisor
	max, divisor = 0, 1e12

	// Indicates whether the deltas can be run-length encoded
	rle = true

	// Iterate in reverse so we can apply deltas in place
	for i := len(deltas) - 1; i > 0; i-- {

		// First differential encode the values
		deltas[i] = deltas[i] - deltas[i-1]

		// We also need to keep track of the max value and largest common divisor
		v := deltas[i]

		if v > max {
			max = v
		}

		// If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
		for divisor > 1 && v%divisor != 0 {
			divisor /= 10
		}

		// Skip the first value || see if prev = curr.  The deltas can be RLE if the are all equal.
		rle = i == len(deltas)-1 || rle && (deltas[i+1] == deltas[i])
	}
	return
}

// Bytes returns the encoded bytes of all written times.
func (e *encoder) Bytes() ([]byte, error) {
	if len(e.ts) == 0 {
		return e.bytes[:0], nil
	}

	// Maximum and largest common divisor.  rle is true if dts (the delta timestamps),
	// are all the same.
	m, div, rle, dts := e.reduce()

	// The deltas are all the same, so we can run-length encode them
	if rle && len(e.ts) > 1 {
		return e.encodeRLE(e.ts[0], e.ts[1], div, len(e.ts))
	}

	// We can't compress this time-range, the deltas exceed 1 << 60
	if m > simple8b.MaxValue {
		return e.encodeRaw()
	}

	return e.encodePacked(div, dts)
}

func (e *encoder) RawBytes() ([]byte, error) {
	if len(e.ts) == 0 {
		return e.bytes[:0], nil
	}
	return e.encodeRaw()
}

func (e *encoder) encodePacked(div uint64, dts []uint64) ([]byte, error) {
	// Only apply the divisor if it's greater than 1 since division is expensive.
	if div > 1 {
		for _, v := range dts[1:] {
			if err := e.enc.Write(v / div); err != nil {
				return nil, err
			}
		}
	} else {
		for _, v := range dts[1:] {
			if err := e.enc.Write(v); err != nil {
				return nil, err
			}
		}
	}

	// The compressed deltas
	deltas, err := e.enc.Bytes()
	if err != nil {
		return nil, err
	}

	sz := 8 + 1 + len(deltas)
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]

	// 4 high bits used for the encoding type
	b[0] = byte(timeCompressedPackedSimple) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	// The first delta value
	binary.BigEndian.PutUint64(b[1:9], uint64(dts[0]))

	copy(b[9:], deltas)
	return b[:9+len(deltas)], nil
}

func (e *encoder) encodeRaw() ([]byte, error) {
	sz := 1 + len(e.ts)*8
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]
	b[0] = byte(timeUncompressed) << 4
	for i, v := range e.ts {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
	}
	return b, nil
}

func (e *encoder) encodeRLE(first, delta, div uint64, n int) ([]byte, error) {
	// Large var int can take up to 10 bytes, we're encoding 3 + 1 byte type
	sz := 31
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]
	// 4 high bits used for the encoding type
	b[0] = byte(timeCompressedRLE) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	i := 1
	// The first timestamp
	binary.BigEndian.PutUint64(b[i:], uint64(first))
	i += 8
	// The first delta
	i += binary.PutUvarint(b[i:], uint64(delta/div))
	// The number of times the delta is repeated
	i += binary.PutUvarint(b[i:], uint64(n))

	return b[:i], nil
}

// TimeDecoder decodes a byte slice into timestamps.
type TimeDecoder struct {
	v    int64
	i, n int
	ts   []uint64
	dec  simple8b.Decoder
	err  error

	// The delta value for a run-length encoded byte slice
	rleDelta int64

	encoding byte
}

// Init initializes the decoder with bytes to read from.
func (d *TimeDecoder) Init(b []byte) {
	d.v = 0
	d.i = 0
	d.ts = d.ts[:0]
	d.err = nil
	if len(b) > 0 {
		// Encoding type is stored in the 4 high bits of the first byte
		d.encoding = b[0] >> 4
	}
	d.decode(b)
}

// Next returns true if there are any timestamps remaining to be decoded.
func (d *TimeDecoder) Next() bool {
	if d.err != nil {
		return false
	}

	if d.encoding == timeCompressedRLE {
		if d.i >= d.n {
			return false
		}
		d.i++
		d.v += d.rleDelta
		return d.i < d.n
	}

	if d.i >= len(d.ts) {
		return false
	}
	d.v = int64(d.ts[d.i])
	d.i++
	return true
}

// Read returns the next timestamp from the decoder.
func (d *TimeDecoder) Read() int64 {
	return d.v
}

// Error returns the last error encountered by the decoder.
func (d *TimeDecoder) Error() error {
	return d.err
}

func (d *TimeDecoder) decode(b []byte) {
	if len(b) == 0 {
		return
	}

	switch d.encoding {
	case timeUncompressed:
		d.decodeRaw(b[1:])
	case timeCompressedRLE:
		d.decodeRLE(b)
	case timeCompressedPackedSimple:
		d.decodePacked(b)
	default:
		d.err = fmt.Errorf("unknown encoding: %v", d.encoding)
	}
}

func (d *TimeDecoder) decodePacked(b []byte) {
	if len(b) < 9 {
		d.err = fmt.Errorf("TimeDecoder: not enough data to decode packed timestamps")
		return
	}
	div := uint64(math.Pow10(int(b[0] & 0xF)))
	first := uint64(binary.BigEndian.Uint64(b[1:9]))

	d.dec.SetBytes(b[9:])

	d.i = 0
	deltas := d.ts[:0]
	deltas = append(deltas, first)

	for d.dec.Next() {
		deltas = append(deltas, d.dec.Read())
	}

	// Compute the prefix sum and scale the deltas back up
	last := deltas[0]
	if div > 1 {
		for i := 1; i < len(deltas); i++ {
			dgap := deltas[i] * div
			deltas[i] = last + dgap
			last = deltas[i]
		}
	} else {
		for i := 1; i < len(deltas); i++ {
			deltas[i] += last
			last = deltas[i]
		}
	}

	d.i = 0
	d.ts = deltas
}

func (d *TimeDecoder) decodeRLE(b []byte) {
	if len(b) < 9 {
		d.err = fmt.Errorf("TimeDecoder: not enough data for initial RLE timestamp")
		return
	}

	var i, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[i] & 0xF)))
	i++

	// Next 8 bytes is the starting timestamp
	first := binary.BigEndian.Uint64(b[i : i+8])
	i += 8

	// Next 1-10 bytes is our (scaled down by factor of 10) run length values
	value, n := binary.Uvarint(b[i:])
	if n <= 0 {
		d.err = fmt.Errorf("TimeDecoder: invalid run length in decodeRLE")
		return
	}

	// Scale the value back up
	value *= uint64(mod)
	i += n

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[i:])
	if n <= 0 {
		d.err = fmt.Errorf("TimeDecoder: invalid repeat value in decodeRLE")
		return
	}

	d.v = int64(first - value)
	d.rleDelta = int64(value)

	d.i = -1
	d.n = int(count)
}

func (d *TimeDecoder) decodeRaw(b []byte) {
	d.i = 0
	d.ts = make([]uint64, len(b)/8)
	for i := range d.ts {
		d.ts[i] = binary.BigEndian.Uint64(b[i*8 : i*8+8])

		delta := d.ts[i]
		// Compute the prefix sum and scale the deltas back up
		if i > 0 {
			d.ts[i] = d.ts[i-1] + delta
		}
	}
}

func CountTimestamps(b []byte) int {
	if len(b) == 0 {
		return 0
	}

	// Encoding type is stored in the 4 high bits of the first byte
	encoding := b[0] >> 4
	switch encoding {
	case timeUncompressed:
		// Uncompressed timestamps are just 8 bytes each
		return len(b[1:]) / 8
	case timeCompressedRLE:
		// First 9 bytes are the starting timestamp and scaling factor, skip over them
		i := 9
		// Next 1-10 bytes is our (scaled down by factor of 10) run length values
		_, n := binary.Uvarint(b[9:])
		i += n
		// Last 1-10 bytes is how many times the value repeats
		count, _ := binary.Uvarint(b[i:])
		return int(count)
	case timeCompressedPackedSimple:
		// First 9 bytes are the starting timestamp and scaling factor, skip over them
		count, _ := simple8b.CountBytes(b[9:])
		return count + 1 // +1 is for the first uncompressed timestamp, starting timestamp in b[1:9]
	default:
		return 0
	}
}

// TimeArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// TimeArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Timestamp values to be encoded should be sorted before encoding.  When encoded,
// the values are first delta-encoded.  The first value is the starting timestamp,
// subsequent values are the difference from the prior value.
//
// Important: TimeArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into TimeArrayEncodeAll.
func TimeArrayEncodeAll(src []int64, b []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil // Nothing to do
	}

	var max, div = uint64(0), uint64(1e12)

	// To prevent an allocation of the entire block we're encoding reuse the
	// src slice to store the encoded deltas.
	deltas := reinterpretInt64ToUint64Slice(src)

	if len(deltas) > 1 {
		for i := len(deltas) - 1; i > 0; i-- {
			deltas[i] = deltas[i] - deltas[i-1]
			if deltas[i] > max {
				max = deltas[i]
			}
		}

		var rle = true
		for i := 2; i < len(deltas); i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		// Deltas are the same - encode with RLE
		if rle {
			// Large varints can take up to 10 bytes.  We're storing 3 + 1
			// type byte.
			if len(b) < 31 && cap(b) >= 31 {
				b = b[:31]
			} else if len(b) < 31 {
				b = append(b, make([]byte, 31-len(b))...)
			}

			// 4 high bits used for the encoding type
			b[0] = byte(timeCompressedRLE) << 4

			i := 1
			// The first value
			binary.BigEndian.PutUint64(b[i:], deltas[0])
			i += 8

			// The first delta, checking the divisor
			// given all deltas are the same, we can do a single check for the divisor
			v := deltas[1]
			for div > 1 && v%div != 0 {
				div /= 10
			}

			if div > 1 {
				// 4 low bits are the log10 divisor
				b[0] |= byte(math.Log10(float64(div)))
				i += binary.PutUvarint(b[i:], deltas[1]/div)
			} else {
				i += binary.PutUvarint(b[i:], deltas[1])
			}

			// The number of times the delta is repeated
			i += binary.PutUvarint(b[i:], uint64(len(deltas)))

			return b[:i], nil
		}
	}

	// We can't compress this time-range, the deltas exceed 1 << 60
	if max > simple8b.MaxValue {
		// Encode uncompressed.
		sz := 1 + len(deltas)*8
		if len(b) < sz && cap(b) >= sz {
			b = b[:sz]
		} else if len(b) < sz {
			b = append(b, make([]byte, sz-len(b))...)
		}

		// 4 high bits of first byte store the encoding type for the block
		b[0] = byte(timeUncompressed) << 4
		for i, v := range deltas {
			binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
		}
		return b[:sz], nil
	}

	// find divisor only if we're compressing with simple8b
	for i := 1; i < len(deltas) && div > 1; i++ {
		// If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
		v := deltas[i]
		for div > 1 && v%div != 0 {
			div /= 10
		}
	}

	// Only apply the divisor if it's greater than 1 since division is expensive.
	if div > 1 {
		for i := 1; i < len(deltas); i++ {
			deltas[i] /= div
		}
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
	b[0] = byte(timeCompressedPackedSimple) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], deltas[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b[:sz], nil
}

var (
	timeBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		timeBatchDecodeAllUncompressed,
		timeBatchDecodeAllSimple,
		timeBatchDecodeAllRLE,
		timeBatchDecodeAllInvalid,
	}
)

func TimeArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > timeCompressedRLE {
		encoding = 3 // timeBatchDecodeAllInvalid
	}

	return timeBatchDecoderFunc[encoding&3](b, dst)
}

func timeBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	for i := range dst {
		dst[i] = int64(binary.BigEndian.Uint64(b[i*8:]))
	}

	return dst, nil
}

func timeBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: not enough data to decode packed timestamps")
	}

	div := uint64(math.Pow10(int(b[0] & 0xF))) // multiplier

	count, err := simple8b.CountBytes(b[9:])
	if err != nil {
		return []int64{}, err
	}

	count += 1

	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	buf := *(*[]uint64)(unsafe.Pointer(&dst))

	// first value
	buf[0] = binary.BigEndian.Uint64(b[1:9])
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[9:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// Compute the prefix sum and scale the deltas back up
	last := buf[0]
	if div > 1 {
		for i := 1; i < len(buf); i++ {
			dgap := buf[i] * div
			buf[i] = last + dgap
			last = buf[i]
		}
	} else {
		for i := 1; i < len(buf); i++ {
			buf[i] += last
			last = buf[i]
		}
	}

	return dst, nil
}

func timeBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: not enough data to decode RLE starting value")
	}

	var k, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[k] & 0xF)))
	k++

	// Next 8 bytes is the starting timestamp
	first := binary.BigEndian.Uint64(b[k:])
	k += 8

	// Next 1-10 bytes is our (scaled down by factor of 10) run length delta
	delta, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("TimeArrayDecodeAll: invalid run length in decodeRLE")
	}
	k += n

	// Scale the delta back up
	delta *= uint64(mod)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("TimeDecoder: invalid repeat value in decodeRLE")
	}

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	acc := first
	for i := range dst {
		dst[i] = int64(acc)
		acc += delta
	}

	return dst, nil
}

func timeBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("unknown encoding %v", b[0]>>4)
}
