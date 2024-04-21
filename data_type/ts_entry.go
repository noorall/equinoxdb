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

package data_type

import (
	"encoding/binary"
	"math"
)

type DataType int

const (
	BOOLEAN DataType = iota
	INT
	FLOAT
	STRING
)

type TSEntry struct {
	dataType   DataType
	key        []byte
	timestamps []byte
	values     []byte
	count      int
}

func New(key []byte, dataType DataType) *TSEntry {
	return &TSEntry{
		dataType:   dataType,
		key:        key,
		timestamps: []byte{},
		values:     []byte{},
		count:      0,
	}
}

func (e *TSEntry) DeepCopy() TSEntry {
	// 复制 []byte 类型的数据，避免共享底层数组
	copyBytes := func(b []byte) []byte {
		if b == nil {
			return nil
		}
		dst := make([]byte, len(b))
		copy(dst, b)
		return dst
	}

	return TSEntry{
		dataType:   e.dataType, // 值类型可以直接复制
		key:        copyBytes(e.key),
		timestamps: copyBytes(e.timestamps),
		values:     copyBytes(e.values),
		count:      e.count, // int 类型是值类型，直接复制
	}
}

func (e *TSEntry) Clean() {
	e.values = e.values[:0]
	e.timestamps = e.timestamps[:0]
	e.count = 0
}

func (e *TSEntry) Size() int {
	return len(e.key) + len(e.timestamps) + len(e.values) + 4 + 4
}

func (e *TSEntry) Count() int {
	return e.count
}

func (e *TSEntry) Key() []byte {
	return e.key
}

func (e *TSEntry) DataType() DataType {
	return e.dataType
}

func (e *TSEntry) Put(timestamp uint64, value interface{}) {
	timeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBytes, timestamp)
	e.timestamps = append(e.timestamps, timeBytes...)

	switch e.dataType {
	case BOOLEAN:
		b, ok := value.(bool)
		if !ok {
			panic("value 类型必须为 bool")
		}
		if b {
			e.values = append(e.values, 1)
		} else {
			e.values = append(e.values, 0)
		}

	case FLOAT:
		f, ok := value.(float64)
		if !ok {
			panic("value 类型必须为 float64")
		}
		bits := math.Float64bits(f) // 转换为 IEEE 754 二进制表示
		floatBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(floatBytes, bits)
		e.values = append(e.values, floatBytes...)

	case STRING:
		s, ok := value.(string)
		if !ok {
			panic("value 类型必须为 string")
		}
		strBytes := []byte(s)
		length := uint32(len(strBytes))
		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, length)
		e.values = append(e.values, lenBytes...)
		e.values = append(e.values, strBytes...)
	case INT:
		i, ok := value.(int64)
		if !ok {
			panic("value 类型必须为 int64")
		}
		intBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(intBytes, uint64(i))
		e.values = append(e.values, intBytes...)
	default:
		panic("unhandled default case")
	}
	e.count++
}

func (e *TSEntry) Get(index int) (uint64, interface{}) {
	if index < 0 || index >= e.count {
		panic("索引超出范围")
	}

	timeStart := index * 8
	timestamp := binary.BigEndian.Uint64(e.timestamps[timeStart : timeStart+8])

	switch e.dataType {
	case BOOLEAN:
		return timestamp, e.values[index] == 1

	case FLOAT:
		valueStart := index * 8
		bits := binary.BigEndian.Uint64(e.values[valueStart : valueStart+8])
		return timestamp, math.Float64frombits(bits)

	case STRING:
		valueStart := index * 4 // 每个字符串前有 4 字节长度
		for i := 0; i < index; i++ {
			lenStart := valueStart
			length := int(binary.BigEndian.Uint32(e.values[lenStart : lenStart+4]))
			valueStart += 4 + length // 跳过长度和字符串内容
		}
		lenStart := valueStart
		length := int(binary.BigEndian.Uint32(e.values[lenStart : lenStart+4]))
		strStart := lenStart + 4
		return timestamp, string(e.values[strStart : strStart+length])
	case INT:
		valueStart := index * 8
		bits := binary.BigEndian.Uint64(e.values[valueStart : valueStart+8])
		return timestamp, int64(bits) // uint64 转为 int64
	}

	return timestamp, nil
}

func (e *TSEntry) GetBytes() []byte {
	return append(e.timestamps, e.values...)
}

func (e *TSEntry) KeyStr() string {
	return string(e.key)
}
