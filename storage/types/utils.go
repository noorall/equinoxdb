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
	"strconv"
	"time"
	"unsafe"
)

const (
	valueTypeUndefined = 0
	valueTypeFloat64   = 1
	valueTypeInteger   = 2
	valueTypeString    = 3
	valueTypeBoolean   = 4
	valueTypeUnsigned  = 5
)

func valueType(v Value) byte {
	switch v.(type) {
	case FloatValue:
		return valueTypeFloat64
	case IntegerValue:
		return valueTypeInteger
	case StringValue:
		return valueTypeString
	case BooleanValue:
		return valueTypeBoolean
	case UnsignedValue:
		return valueTypeUnsigned
	default:
		return valueTypeUndefined
	}
}

func parseIntBytes(b []byte, base int, bitSize int) (i int64, err error) {
	s := unsafeBytesToString(b)
	return strconv.ParseInt(s, base, bitSize)
}

func parseFloatBytes(b []byte, bitSize int) (float64, error) {
	s := unsafeBytesToString(b)
	return strconv.ParseFloat(s, bitSize)
}

func parseBoolBytes(b []byte) (bool, error) {
	return strconv.ParseBool(unsafeBytesToString(b))
}

func parseUintBytes(b []byte, base int, bitSize int) (i uint64, err error) {
	s := unsafeBytesToString(b)
	return strconv.ParseUint(s, base, bitSize)
}

func unsafeBytesToString(in []byte) string {
	return *(*string)(unsafe.Pointer(&in))
}

func LifeCycleToUnixNano(l int) int64 {
	switch LifeCycle(l) {
	case LifeCycleSevenDays:
		return (7 * 24 * time.Hour).Nanoseconds()
	case LifeCycleFourteenDays:
		return (14 * 24 * time.Hour).Nanoseconds()
	case LifeCycleOneMouth:
		return (30 * 24 * time.Hour).Nanoseconds()
	case LifeCycleSixMouth:
		return (6 * 30 * 24 * time.Hour).Nanoseconds()
	case LifeCycleOneYear:
		return (12 * 30 * 24 * time.Hour).Nanoseconds()
	default:
		return -1
	}
}
