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

type BooleanValue struct {
	unixNano int64
	value    bool
}

// NewBooleanValue returns a new boolean value.
func NewBooleanValue(t int64, v bool) Value {
	return BooleanValue{unixNano: t, value: v}
}

func (v BooleanValue) Size() int {
	return 9
}

func (v BooleanValue) UnixNano() int64 {
	return v.unixNano
}

func (v BooleanValue) Value() interface{} {
	return v.value
}

func (v BooleanValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixNano), v.Value())
}

func (v BooleanValue) RawValue() bool { return v.value }

func EncodeBooleanBlockUsing(buf []byte, values []Value, tenc codec.TimeEncoder, venc codec.BooleanEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(BooleanValue)
		tenc.Write(vv.unixNano)
		venc.Write(vv.value)
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
