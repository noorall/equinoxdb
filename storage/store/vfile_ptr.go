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

package store

import (
	"encoding/binary"
	"fmt"
)

const (
	ValuePtrSize = 34
)

type ValuePtr struct {
	MinTime, MaxTime int64
	Offset           int64
	FileNo           uint32
	Size             uint32
	LifeCycle        int
}

func (v *ValuePtr) AppendTo(b []byte) []byte {
	if len(b) < ValuePtrSize {
		if cap(b) < ValuePtrSize {
			b = make([]byte, ValuePtrSize)
		} else {
			b = b[:ValuePtrSize]
		}
	}

	binary.BigEndian.PutUint64(b[:8], uint64(v.MinTime))
	binary.BigEndian.PutUint64(b[8:16], uint64(v.MaxTime))
	binary.BigEndian.PutUint64(b[16:24], uint64(v.Offset))
	binary.BigEndian.PutUint32(b[24:28], v.FileNo)
	binary.BigEndian.PutUint32(b[28:32], v.Size)
	binary.BigEndian.PutUint16(b[32:34], uint16(v.LifeCycle))

	return b
}

func (v *ValuePtr) UnmarshalBinary(b []byte) error {
	if len(b) < ValuePtrSize {
		return fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), ValuePtrSize)
	}
	v.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	v.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
	v.Offset = int64(binary.BigEndian.Uint64(b[16:24]))
	v.FileNo = binary.BigEndian.Uint32(b[24:28])
	v.Size = binary.BigEndian.Uint32(b[28:32])
	v.LifeCycle = int(binary.BigEndian.Uint16(b[32:34]))
	return nil
}

func DecodeValuePtrs(b []byte) ([]*ValuePtr, error) {
	if len(b) < ValuePtrSize {
		return nil, fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), ValuePtrSize)
	}
	count := len(b) / ValuePtrSize
	ptrs := make([]*ValuePtr, count)
	for i := 0; i < count; i++ {
		v := &ValuePtr{}
		_ = v.UnmarshalBinary(b[i*ValuePtrSize : (i+1)*ValuePtrSize])
		ptrs[i] = v
	}
	return ptrs, nil
}
