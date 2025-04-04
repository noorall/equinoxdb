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
	VFileHeaderSize = 30
)

type VFileHeader struct {
	KeyLen           uint16
	DataLen          uint32
	KeyHash          uint64
	MinTime, MaxTime int64
}

func (f *VFileHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, VFileHeaderSize)

	binary.BigEndian.PutUint64(b[:8], f.KeyHash)
	binary.BigEndian.PutUint64(b[8:16], uint64(f.MinTime))
	binary.BigEndian.PutUint64(b[16:24], uint64(f.MaxTime))
	binary.BigEndian.PutUint32(b[24:28], f.DataLen)
	binary.BigEndian.PutUint16(b[28:30], f.KeyLen)

	return b, nil
}

func (f *VFileHeader) UnmarshalBinary(b []byte) error {
	if len(b) < VFileHeaderSize {
		return fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), VFileHeaderSize)
	}
	f.KeyHash = binary.BigEndian.Uint64(b[:8])
	f.MinTime = int64(binary.BigEndian.Uint64(b[8:16]))
	f.MaxTime = int64(binary.BigEndian.Uint64(b[16:24]))
	f.DataLen = binary.BigEndian.Uint32(b[24:28])
	f.KeyLen = binary.BigEndian.Uint16(b[28:30])
	if f.KeyLen == 0 && f.MinTime == 0 && f.MaxTime == 0 && f.DataLen == 0 && f.KeyHash == 0 {
		return fmt.Errorf("unmarshalBinary: invalid empty header")
	}
	return nil
}
