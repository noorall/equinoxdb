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

type StringValue struct {
	unixNano int64
	value    string
}

// NewStringValue returns a new string value.
func NewStringValue(t int64, v string) Value {
	return StringValue{unixNano: t, value: v}
}

func (v StringValue) Value() interface{} {
	return v.value
}

func (v StringValue) UnixNano() int64 {
	return v.unixNano
}

func (v StringValue) Size() int {
	return 8 + len(v.value)
}

func (v StringValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixNano), v.Value())
}

func (v StringValue) RawValue() string { return v.value }

func EncodeStringBlockUsing(buf []byte, values []Value, tenc codec.TimeEncoder, venc codec.StringEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	for _, v := range values {
		vv := v.(StringValue)
		tenc.Write(vv.unixNano)
		venc.Write(vv.value)
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
