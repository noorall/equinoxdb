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

type IntegerValue struct {
	unixNano int64
	value    int64
}

// NewIntegerValue returns a new integer value.
func NewIntegerValue(t int64, v int64) Value {
	return IntegerValue{unixNano: t, value: v}
}

func (v IntegerValue) Value() interface{} {
	return v.value
}

func (v IntegerValue) UnixNano() int64 {
	return v.unixNano
}

func (v IntegerValue) Size() int {
	return 16
}

func (v IntegerValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixNano), v.Value())
}

func (v IntegerValue) RawValue() int64 { return v.value }

func EncodeIntegerBlockUsing(buf []byte, values []Value, tenc codec.TimeEncoder, venc codec.IntegerEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(IntegerValue)
		tenc.Write(vv.unixNano)
		venc.Write(vv.value)
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
