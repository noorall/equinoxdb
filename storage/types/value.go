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
	"fmt"
	"time"
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

	// encodedBlockHeaderSize is the size of the header for an encoded block.  There is one
	// byte encoding the type of the block.
	encodedBlockHeaderSize = 1
)

type Value interface {
	UnixNano() int64

	Value() interface{}

	Size() int

	String() string
}

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

type UnsignedValue struct {
	unixNano int64
	value    uint64
}

// NewUnsignedValue returns a new unsigned integer value.
func NewUnsignedValue(t int64, v uint64) Value {
	return UnsignedValue{unixNano: t, value: v}
}

func (v UnsignedValue) Value() interface{} {
	return v.value
}

func (v UnsignedValue) UnixNano() int64 {
	return v.unixNano
}

func (v UnsignedValue) Size() int {
	return 16
}

func (v UnsignedValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixNano), v.Value())
}

func (v UnsignedValue) RawValue() uint64 { return v.value }
