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
	"fmt"
)

const (
	SSTFileExtension = "sst"

	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	MagicNumber uint32 = 0x16D116D1

	// Version indicates the version of the TSM file format.
	Version byte = 1

	// Size in bytes of an index entry
	indexEntrySize = 28

	// Size in bytes used to store the count of index entries for a key
	indexCountSize = 2

	// Size in bytes used to store the type of block encoded
	indexTypeSize = 1

	// Max number of blocks for a given key that can exist in a single file
	maxIndexEntries = (1 << (indexCountSize * 8)) - 1

	// max length of a key in an index entry (measurement + tags)
	maxKeyLength = (1 << (2 * 8)) - 1

	// The threshold amount data written before we periodically fsync a TSM file.  This helps avoid
	// long pauses due to very large fsyncs at the end of writing a TSM file.
	fsyncEvery = 25 * 1024 * 1024
)

// TSMFile represents an on-disk TSM file.
type TSMFile interface {
	// Path returns the underlying file path for the TSMFile.  If the file
	// has not be written or loaded from disk, the zero value is returned.
	Path() string

	// Read returns all the values in the block where time t resides.
	Read(key []byte, t int64) ([]types.Value, error)

	// ReadAt returns all the values in the block identified by entry.
	ReadAt(entry *IndexEntry, values []types.Value) ([]types.Value, error)
	ReadFloatBlockAt(entry *IndexEntry, values *[]types.FloatValue) ([]types.FloatValue, error)
	ReadFloatArrayBlockAt(entry *IndexEntry, values *types.FloatArray) error
	ReadIntegerBlockAt(entry *IndexEntry, values *[]types.IntegerValue) ([]types.IntegerValue, error)
	ReadIntegerArrayBlockAt(entry *IndexEntry, values *types.IntegerArray) error
	ReadUnsignedBlockAt(entry *IndexEntry, values *[]types.UnsignedValue) ([]types.UnsignedValue, error)
	ReadUnsignedArrayBlockAt(entry *IndexEntry, values *types.UnsignedArray) error
	ReadStringBlockAt(entry *IndexEntry, values *[]types.StringValue) ([]types.StringValue, error)
	ReadStringArrayBlockAt(entry *IndexEntry, values *types.StringArray) error
	ReadBooleanBlockAt(entry *IndexEntry, values *[]types.BooleanValue) ([]types.BooleanValue, error)
	ReadBooleanArrayBlockAt(entry *IndexEntry, values *types.BooleanArray) error

	// Entries returns the index entries for all blocks for the given key.
	Entries(key []byte) []IndexEntry
	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	// Returns true if the TSMFile may contain a value with the specified
	// key and time.
	ContainsValue(key []byte, t int64) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key []byte) bool

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the key range of the file intersects min and max.
	OverlapsKeyRange(min, max []byte) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// KeyCount returns the number of distinct keys in the file.
	KeyCount() int

	// Seek returns the position in the index with the key <= key.
	Seek(key []byte) int

	// KeyAt returns the key located at index position idx.
	KeyAt(idx int) ([]byte, byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// BatchDelete return a BatchDeleter that allows for multiple deletes in batches
	// and group commit or rollback.
	BatchDelete() BatchDeleter

	// Delete removes the keys from the set of keys available in this file.
	Delete(keys [][]byte) error

	// DeleteRange removes the values for keys between timestamps min and max.
	DeleteRange(keys [][]byte, min, max int64) error

	// HasTombstones returns true if file contains values that have been deleted.
	HasTombstones() bool

	// Close closes the underlying file resources.
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() uint32

	// Rename renames the existing TSM file to a new name and replaces the mmap backing slice using the new
	// file name. Index and Reader state are not re-initialized.
	Rename(path string) error

	// Remove deletes the file from the filesystem.
	Remove() error

	// InUse returns true if the file is currently in use by queries.
	InUse() bool

	// Ref records that this file is actively in use.
	Ref()

	// Unref records that this file is no longer in use.
	Unref()

	// BlockIterator returns an iterator pointing to the first block in the file and
	// allows sequential iteration to each and every block.
	BlockIterator() *BlockIterator

	// Free releases any resources held by the FileStore to free up system resources.
	Free() error
}

type BlockIterator struct {
	r *TSMReader

	// i is the current key index
	i int

	// n is the total number of keys
	n int

	key     []byte
	cache   []IndexEntry
	entries []IndexEntry
	err     error
	typ     byte
}

// PeekNext returns the next key to be iterated or an empty string.
func (b *BlockIterator) PeekNext() []byte {
	if len(b.entries) > 1 {
		return b.key
	} else if b.n-b.i > 1 {
		key, _ := b.r.KeyAt(b.i + 1)
		return key
	}
	return nil
}

// Next returns true if there are more blocks to iterate through.
func (b *BlockIterator) Next() bool {
	if b.err != nil {
		return false
	}

	if b.n-b.i == 0 && len(b.entries) == 0 {
		return false
	}

	if len(b.entries) > 0 {
		b.entries = b.entries[1:]
		if len(b.entries) > 0 {
			return true
		}
	}

	if b.n-b.i > 0 {
		b.key, b.typ, b.entries = b.r.Key(b.i, &b.cache)
		b.i++

		// If there were deletes on the TSMReader, then our index is now off and we
		// can't proceed.  What we just read may not actually the next block.
		if b.n != b.r.KeyCount() {
			b.err = fmt.Errorf("delete during iteration")
			return false
		}

		if len(b.entries) > 0 {
			return true
		}
	}

	return false
}

// Read reads information about the next block to be iterated.
func (b *BlockIterator) Read() (key []byte, minTime int64, maxTime int64, typ byte, checksum uint32, buf []byte, err error) {
	if b.err != nil {
		return nil, 0, 0, 0, 0, nil, b.err
	}
	checksum, buf, err = b.r.ReadBytes(&b.entries[0], nil)
	if err != nil {
		b.err = err
		return nil, 0, 0, 0, 0, nil, err
	}
	return b.key, b.entries[0].MinTime, b.entries[0].MaxTime, b.typ, checksum, buf, err
}

// Err returns any errors encounter during iteration.
func (b *BlockIterator) Err() error {
	return b.err
}
