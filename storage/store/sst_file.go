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
	"equinox/storage/types"
	"io"
)

const (
	SSTFileExtension = "sst"
)

type SSTWriter interface {
	Write(key []byte, values types.Values) error

	WriteBlock(key []byte, minTime, maxTime int64, block []byte) error

	WriteIndex() error

	Flush() error

	Close() error

	Size() uint32

	Remove() error
}

type IndexWriter interface {
	Add(key []byte, blockType byte, minTime, maxTime int64, offset int64, size uint32)

	Entries(key []byte) []IndexEntry

	KeyCount() int

	Size() uint32

	MarshalBinary() ([]byte, error)

	WriteTo(w io.Writer) (int64, error)

	Close() error

	Remove() error
}

type IndexEntry struct {
	MinTime, MaxTime int64

	Offset int64

	Size uint32
}
