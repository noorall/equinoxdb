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

package types

import (
	"equinox/storage/codec"
	"fmt"
	"time"
)

type FloatValue struct {
	unixNano int64
	value    float64
}

// NewFloatValue returns a new float value.
func NewFloatValue(t int64, v float64) Value {
	return FloatValue{unixNano: t, value: v}
}

func (v FloatValue) UnixNano() int64 {
	return v.unixNano
}

func (v FloatValue) Value() interface{} {
	return v.value
}

func (v FloatValue) Size() int {
	return 16
}

func (v FloatValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixNano), v.value)
}

func (v FloatValue) RawValue() float64 { return v.value }

func EncodeFloatBlockUsing(buf []byte, values []Value, tsenc codec.TimeEncoder, venc *codec.FloatEncoder) ([]byte, error) {
	tsenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(FloatValue)
		tsenc.Write(vv.unixNano)
		venc.Write(vv.value)
	}
	venc.Flush()

	// Encoded timestamp values
	tb, err := tsenc.Bytes()
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
	return packBlock(buf, BlockFloat64, tb, vb), nil
}
