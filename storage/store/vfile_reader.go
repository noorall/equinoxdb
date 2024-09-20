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
	"fmt"
	"hash/crc32"
)

type ValueFileIterator struct {
	f *ValueFile

	offset        uint32
	lenValueBlock uint32
	header        *VFileHeader
}

func NewValueFileIterator(f *ValueFile) *ValueFileIterator {
	return &ValueFileIterator{f: f, header: &VFileHeader{}}
}

func (r *ValueFileIterator) Next() error {
	if r.offset+VFileHeaderSize > r.f.size {
		return fmt.Errorf("no next left")
	}
	b := r.f.Data[r.offset : r.offset+VFileHeaderSize]
	err := r.header.UnmarshalBinary(b)
	if err != nil {
		return err
	}
	r.lenValueBlock = uint32(r.header.KeyLen) + crc32.Size + r.header.DataLen
	r.offset = r.offset + VFileHeaderSize + r.lenValueBlock
	return nil
}

func (r *ValueFileIterator) ReadHeader() *VFileHeader {
	return r.header
}

func (r *ValueFileIterator) ReadKey() []byte {
	start := r.offset - r.lenValueBlock
	return r.f.Data[start : start+uint32(r.header.KeyLen)]
}

func (r *ValueFileIterator) ReadValueBlock() []byte {
	start := r.offset - r.lenValueBlock + uint32(r.header.KeyLen) + crc32.Size
	return r.f.Data[start : start+r.header.DataLen]
}
