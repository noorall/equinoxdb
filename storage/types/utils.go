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

import "encoding/binary"

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

func packBlock(buf []byte, typ byte, ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	sz := 1 + binary.MaxVarintLen64 + len(ts) + len(values)
	if cap(buf) < sz {
		buf = make([]byte, sz)
	}
	b := buf[:sz]
	b[0] = typ
	i := binary.PutUvarint(b[1:1+binary.MaxVarintLen64], uint64(len(ts)))
	i += 1

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	copy(b[i:], ts)
	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	copy(b[i+len(ts):], values)
	return b[:i+len(ts)+len(values)]
}
