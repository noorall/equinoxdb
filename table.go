package equinox

import (
	"bytes"
	"equinox/internel"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"

	"errors"
)

const tableFile = ".sst"

type tableIndex struct {
	blockOffsets     []uint32
	baseKeys         [][]byte
	blockLens        []uint32
	bloom            internel.BloomFilter
	maxVersion       uint64
	uncompressedSize uint32
	onDiskSize       uint32
	keyCount         uint32
}

func (t *tableIndex) Encode() []byte {
	sz := len(t.blockOffsets)*4 + len(t.blockLens)*4 + len(t.bloom) + 4 /*bloom lenght*/ +
		4 /*number of triples*/ + 4 /*uncompression size*/ +
		8 /*max version*/ + 4 /*keys count*/ + 4 /*disk size*/
	n := len(t.baseKeys)
	for i := 0; i < n; i++ {
		sz += len(t.baseKeys[i]) + 4 /*length of key*/
	}

	t.onDiskSize += uint32(sz)

	written := 0
	buf := make([]byte, sz)

	// writes the given key,offset,len triple by reverse order, read by order
	for i := n - 1; i >= 0; i-- {
		written += copy(buf[written:], t.baseKeys[i])
		written += copy(buf[written:], internel.U32ToBytes(uint32(len(t.baseKeys[i]))))
		written += copy(buf[written:], internel.U32ToBytes(t.blockOffsets[i]))
		written += copy(buf[written:], internel.U32ToBytes(t.blockLens[i]))
	}

	// writes number of these triples
	written += copy(buf[written:], internel.U32ToBytes(uint32(n)))

	// writes bloomfilter and its length
	written += copy(buf[written:], t.bloom)
	written += copy(buf[written:], internel.U32ToBytes(uint32(len(t.bloom))))

	// writes max version
	written += copy(buf[written:], internel.U64ToBytes(t.maxVersion))

	// writes uncompression size
	written += copy(buf[written:], internel.U32ToBytes(t.uncompressedSize))

	// writes keys count
	written += copy(buf[written:], internel.U32ToBytes(t.keyCount))

	// writes disk size
	written += copy(buf[written:], internel.U32ToBytes(t.onDiskSize))

	return buf
}

func (t *tableIndex) Decode(buf []byte) {
	readPos := uint32(len(buf))
	// read disk size
	t.onDiskSize = internel.BytesToU32(buf[readPos-4 : readPos])
	readPos -= 4

	// read keys count
	t.keyCount = internel.BytesToU32(buf[readPos-4 : readPos])
	readPos -= 4

	// read uncompression size
	t.uncompressedSize = internel.BytesToU32(buf[readPos-4 : readPos])
	readPos -= 4

	// read max version
	t.maxVersion = internel.BytesToU64(buf[readPos-8 : readPos])
	readPos -= 8

	// read bloomfilter
	bloomLen := internel.BytesToU32(buf[readPos-4 : readPos])
	readPos -= 4
	t.bloom = buf[readPos-bloomLen : readPos]
	readPos -= bloomLen

	// read nums of these triples
	triples := internel.BytesToU32(buf[readPos-4 : readPos])
	readPos -= 4

	t.blockLens = make([]uint32, triples)
	t.blockOffsets = make([]uint32, triples)
	t.baseKeys = make([][]byte, triples)

	for i := uint32(0); i < triples; i++ {
		// read length of block
		t.blockLens[i] = internel.BytesToU32(buf[readPos-4 : readPos])
		readPos -= 4
		// read offset of block
		t.blockOffsets[i] = internel.BytesToU32(buf[readPos-4 : readPos])
		readPos -= 4
		// read baseKey of block
		keyLen := internel.BytesToU32(buf[readPos-4 : readPos])
		readPos -= 4
		t.baseKeys[i] = buf[readPos-keyLen : readPos]
		readPos -= keyLen
	}
}

type Table struct {
	sync.Mutex
	*internel.MmapFile

	tableSize uint32 // Initialized in OpenTable, using fd.Stat().

	index *tableIndex
	ref   int32 // For file garbage collection. Atomic.

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys (with timestamps).
	id                uint64 // file id, part of filename

	Checksum       []byte
	CreatedAt      time.Time
	indexStart     uint32
	indexLen       uint32
	hasBloomFilter bool

	IsInmemory bool // Set to true if the table is on level 0 and opened in memory.
	opt        *Options
}

func CreateTable(fname string, builder *TableBuilder) (*Table, error) {
	fbs := builder.Done()
	mf, err := internel.OpenMmapFile(fname, os.O_CREATE|os.O_RDWR|os.O_EXCL, fbs.size)
	if err != nil {
		return nil, Wrapf(err, "while creating table: %s", fname)
	}

	if !mf.NewFile {
		return nil, fmt.Errorf("file already exist: %s", fname)
	}

	written := fbs.Copy(mf.Data)
	AssertTrue(written == len(mf.Data))

	if err := mf.Sync(); err != nil {
		return nil, Wrapf(err, "while msync on: %s", fname)
	}

	return OpenTable(mf, *builder.opts)
}

func OpenTable(mf *internel.MmapFile, opts Options) (*Table, error) {
	if opts.BlockSize == 0 && opts.CompressionType != NoCompression {
		return nil, errors.New("Block size cannot be zero")
	}

	fileInfo, err := mf.Fd.Stat()
	if err != nil {
		mf.Close()
		return nil, err
	}

	filename := fileInfo.Name()
	id, ok := GetFileID(filename)
	if !ok {
		mf.Close()
		return nil, fmt.Errorf("Invalid filename: %s", filename)
	}

	t := &Table{
		MmapFile:   mf,
		ref:        1, // Caller is given one reference.
		id:         id,
		opt:        &opts,
		IsInmemory: false,
		tableSize:  uint32(fileInfo.Size()),
		CreatedAt:  fileInfo.ModTime(),
	}

	if err := t.getBiggestAndSmallest(); err != nil {
		return nil, Wrapf(err, "failed to initialize table")
	}

	if opts.VerifyTableChecksum {
		if err := t.VerifyCheckSum(); err != nil {
			mf.Close()
			return nil, Wrapf(err, "failed to verify checksum")
		}
	}

	return t, nil
}

func OpenInMemoryTable(data []byte, id uint64, opt *Options) (*Table, error) {
	mf := &internel.MmapFile{
		Data: data,
		Fd:   nil,
	}
	t := &Table{
		MmapFile:   mf,
		ref:        1, // Caller is given one reference.
		opt:        opt,
		tableSize:  uint32(len(data)),
		IsInmemory: true,
		id:         id, // It is important that each table gets a unique ID.
	}

	if err := t.getBiggestAndSmallest(); err != nil {
		return nil, err
	}

	return t, nil
}

// IncrRef increments the refcount (having to do with whether the file should be deleted)
func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *Table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// We can safely delete this file, because for all the current files, we always have
		// at least one reference pointing to them.
		go func() {
			if err := t.Delete(); err != nil {
				t.opt.Logger.Warningf("Failed to delete table", err)
			}
			t.opt.Logger.Debugf("table %d deleted ", t.id)
		}()
	}

	return nil
}

// MaxVersion returns the maximum version across all keys stored in this table.
func (t *Table) MaxVersion() uint64 { return t.index.maxVersion }

// IndexSize is the size of table index in bytes.
func (t *Table) IndexSize() int {
	return int(t.indexLen)
}

// BloomFilterSize returns the size of the bloom filter in bytes stored in memory.
func (t *Table) BloomFilterSize() int { return len(t.index.bloom) }

// UncompressedSize is the size uncompressed data stored in this file.
func (t *Table) UncompressedSize() uint32 { return t.index.uncompressedSize }

// KeyCount is the total number of keys in this table.
func (t *Table) KeyCount() uint32 { return t.index.keyCount }

// OnDiskSize returns the total size of key-values stored in this table (including the
// disk space occupied on the value log).
func (t *Table) OnDiskSize() uint32 { return t.index.onDiskSize }

// CompressionType returns the compression algorithm used for block compression.
func (t *Table) CompressionType() CompressionType {
	return t.opt.CompressionType
}

// BlockCount returns the number of block in a table
func (t *Table) BlockCount() int { return len(t.index.blockOffsets) }

// Size is its file size in bytes
func (t *Table) Size() int64 { return int64(t.tableSize) }

// Smallest is its smallest key, or nil if there are none
func (t *Table) Smallest() []byte { return t.smallest }

// Biggest is its biggest key, or nil if there are none
func (t *Table) Biggest() []byte { return t.biggest }

// Filename is NOT the file name.  Just kidding, it is.
func (t *Table) Filename() string { return t.Fd.Name() }

// ID is the table's ID number (used to make the file name).
func (t *Table) ID() uint64 { return t.id }

// DoesNotHave returns true if and only if the table does not have the key hash.
// It does a bloom filter lookup.
func (t *Table) DoesNotHave(hash uint32) bool {
	if !t.hasBloomFilter {
		return false
	}

	bf := t.index.bloom
	return !bf.MayContainHash(hash)
}

// KeySplits will split table at most n parts by prefix that base key matched.
func (t *Table) KeySplits(n int, prefix []byte) []string {
	if n == 0 {
		return nil
	}

	numKeys := len(t.index.baseKeys)
	step := numKeys / n
	if step == 0 {
		step = 1
	}

	var res []string
	for i := 0; i < numKeys; i += step {
		if i >= numKeys {
			i = numKeys - 1
		}

		key := t.index.baseKeys[i]
		if bytes.HasPrefix(key, prefix) {
			res = append(res, string(key))
		}
	}

	return res
}

// VerifyCheckSum will Verify that all blocks in the table are valid by the checksum
func (t *Table) VerifyCheckSum() error {
	numBlocks := len(t.index.blockOffsets)

	for i := 0; i < numBlocks; i++ {
		blk, err := t.getBlockAt(i, true)
		if blk == nil {
			blk = &memBlock{offset: -1}
		}

		if err != nil {
			return Wrapf(err, "checksum validation failed for table: %s, block: %d, offset:%d",
				t.Fd.Name(), i, blk.offset)
		}

		defer blk.decrRef()
	}

	return nil
}

func (t *Table) getBlockAt(idx int, checksum bool) (*memBlock, error) {
	if idx >= len(t.index.blockOffsets) {
		return nil, errors.New("block out of index")
	}

	blockOffset := t.index.blockOffsets[idx]
	blockLen := t.index.blockLens[idx]
	blk := &memBlock{
		offset: int(blockOffset),
		ref:    1,
	}
	defer blk.decrRef()
	atomic.AddInt32(&NumMemBlocks, 1)

	blk.data = t.Data[blockOffset : blockOffset+blockLen]
	if err := t.decompress(blk); err != nil {
		return nil, Wrapf(err,
			"failed to decode compressed data in file: %s at offset: %d, len: %d",
			t.Fd.Name(), blockOffset, blockLen)
	}

	// read length of checksum
	readPos := len(blk.data)
	blk.chkLen = int(internel.BytesToU32(blk.data[readPos-4 : readPos]))
	readPos -= 4

	if blk.chkLen > len(blk.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	// read checksum
	blk.checksum = blk.data[readPos-blk.chkLen : readPos]
	readPos -= blk.chkLen

	// read number of entries
	numEntries := int(internel.BytesToU32(blk.data[readPos-4 : readPos]))
	readPos -= 4

	// read offsets of entries
	start, end := readPos-4*numEntries, readPos
	blk.entryOffsets = internel.BytesToU32Slice(blk.data[start:end])
	blk.entriesIndexStart = start
	blk.data = blk.data[:readPos+4] // without checksum
	// verify block checksum
	if checksum {
		if err := blk.verifyCheckSum(); err != nil {
			return nil, err
		}
	}

	blk.incrRef()

	return blk, nil
}

func (t *Table) getBiggestAndSmallest() error {
	if err := t.initIndex(); err != nil {
		return err
	}

	// get entry by the iterator
	iter := t.NewIterator(false)

	// smallest key
	iter.SeekToFirst()
	if !iter.Valid() {
		return iter.err
	}
	key := iter.Key()
	// deep copy
	t.smallest = make([]byte, len(key))
	copy(t.smallest, key)

	// biggest key
	iter.SeekToLast()
	if !iter.Valid() {
		return iter.err
	}
	key = iter.Key()
	// deep copy
	t.biggest = make([]byte, len(key))
	copy(t.biggest, key)

	return iter.Close()
}

func (t *Table) initIndex() error {
	readPos := uint32(t.tableSize)
	// Read checksum len from the last 4 bytes. crc32 has 4 bytes
	checksumLen := int(internel.BytesToU32(t.Data[readPos-4 : readPos]))
	readPos -= 4
	if checksumLen < 0 {
		return errors.New("checksum length less than zero. Data corrupted")
	}

	// Read checksum.
	checksum := t.Data[readPos-uint32(checksumLen) : readPos]
	readPos -= uint32(checksumLen)

	// read index size
	t.indexLen = internel.BytesToU32(t.Data[readPos-4 : readPos])
	readPos -= 4

	// read index
	indexData := t.Data[readPos-t.indexLen : readPos]
	readPos -= t.indexLen
	t.indexStart = readPos

	// verify index checksum
	if !internel.VerifyCheckSum(indexData, checksum) {
		return Wrapf(ErrChecksumMismatch, "failed to verify checksum for table: %s", t.Fd.Name())
	}

	t.index = &tableIndex{}
	t.index.Decode(indexData)
	t.hasBloomFilter = len(t.index.bloom) > 0

	return nil
}

func (t *Table) decompress(b *memBlock) error {
	var dst []byte
	var err error

	data := b.data
	switch t.opt.CompressionType {
	case NoCompression:
		return nil
	case SnappyCompression:
		if sz, err := snappy.DecodedLen(b.data); err == nil {
			dst = make([]byte, sz)
		} else {
			dst = make([]byte, 4*len(data))
		}

		b.data, err = snappy.Decode(dst, b.data)
		if err != nil {
			return Wrapf(err, "failed to decompress")
		}
	case ZSTDCompression:
		sz := int(float64(t.opt.BlockSize) * 1.2)
		dst = make([]byte, sz)
		b.data, err = internel.ZSTDDecompress(dst, b.data)
		if err != nil {
			return Wrapf(err, "failed to decompress")
		}
	default:
		return ErrCompressionType
	}

	return nil
}

func GetFileID(fname string) (uint64, bool) {
	name := filepath.Base(fname)
	if !strings.HasSuffix(name, tableFile) {
		return 0, false
	}

	name = strings.TrimSuffix(name, tableFile)
	id, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, false
	}

	return id, true
}

func NewTableFileName(id uint64, dir string) string {
	return filepath.Join(dir, GetFileName(id))
}

func GetFileName(id uint64) string {
	return fmt.Sprintf("%06d%s", id, tableFile)
}

/*
The table structure looks like
+---------+------------+-----------+---------------+
| Block 1 | Block 2    | Block 3   | Block 4       |
+---------+------------+-----------+---------------+
| Block 5 | Block 6    | Block ... | Block N       |
+---------+------------+-----------+---------------+
| Index   | Index Size | Checksum  | Checksum Size |
+---------+------------+-----------+---------------+
*/
type TableBuilder struct {
	blkBuilder *blockBuilder
	fBlocks    *fileBlocks

	// Used to concurrently compress/encrypt blocks.
	wg        sync.WaitGroup
	blockChan chan *fileBlock
	opts      *Options

	capacity uint64
}

func NewTableBuilder(opts Options) *TableBuilder {
	b := &TableBuilder{}
	b.blkBuilder = &blockBuilder{curBlock: &fileBlock{}, opts: &opts}
	b.fBlocks = &fileBlocks{}
	b.opts = &opts
	b.capacity = uint64(0.95 * float64(opts.tableSize))

	if opts.CompressionType == NoCompression {
		return b
	}

	count := 2 * runtime.NumCPU()
	b.blockChan = make(chan *fileBlock, count*2)
	b.wg.Add(count)

	for i := 0; i < count; i++ {
		go b.compressBlock()
	}

	return b
}

func (b *TableBuilder) compressBlock() {
	defer b.wg.Done()

	doCompress := b.opts.CompressionType != NoCompression

	for block := range b.blockChan {
		// Extract the block.
		blockBuf := block.data[:block.end]
		// Compress the block.
		if doCompress {
			out, err := b.compressData(blockBuf)

			Check(err)
			blockBuf = out
		}

		block.data = blockBuf
		block.end = len(blockBuf)

		atomic.AddUint32(&b.blkBuilder.compressedSize, uint32(len(blockBuf)))
	}
}

func (b *TableBuilder) compressData(data []byte) ([]byte, error) {
	switch b.opts.CompressionType {
	case NoCompression:
		return data, nil
	case SnappyCompression:
		sz := snappy.MaxEncodedLen(len(data))
		dst := make([]byte, sz)
		return snappy.Encode(dst, data), nil
	case ZSTDCompression:
		sz := internel.ZSTDCompressBound(len(data))
		dst := make([]byte, sz)
		return internel.ZSTDCompress(dst, data, b.opts.ZSTDCompressionLevel)
	}

	return nil, ErrCompressionType
}

func (b *TableBuilder) Add(key []byte, value EValue, valueLen uint32) {
	blk := b.blkBuilder.Add(key, value, valueLen)

	if blk == nil {
		return
	}
	b.fBlocks.blockList = append(b.fBlocks.blockList, blk)
	if b.blockChan != nil {
		b.blockChan <- blk
	}
}

func (b *TableBuilder) Empty() bool {
	return b.blkBuilder.Empty()
}

func (b *TableBuilder) Finish() []byte {
	fbs := b.Done()
	buf := make([]byte, fbs.size)
	written := fbs.Copy(buf)
	AssertTrue(written == len(buf))

	return buf
}

func (b *TableBuilder) ReachedCapacity() bool {
	sumBlockSizes := atomic.LoadUint32(&b.blkBuilder.compressedSize)
	if b.opts.CompressionType == NoCompression {
		sumBlockSizes = b.blkBuilder.uncompressedSize
	}
	blocksSize := sumBlockSizes + // actual length of current buffer
		uint32(len(b.blkBuilder.curBlock.entryOffsets)*4) + // all entry offsets size
		4 + // count of all entry offsets
		8 + // checksum bytes
		4 // checksum length

	estimateSz := blocksSize +
		4 + // Index length
		b.blkBuilder.lenOffsets

	return uint64(estimateSz) > b.capacity
}

func (b *TableBuilder) Done() fileBlocks {
	blk := b.blkBuilder.Finish()
	if blk != nil {
		b.fBlocks.blockList = append(b.fBlocks.blockList, blk)
		if b.blockChan != nil {
			b.blockChan <- blk
		}
	}

	if b.blockChan != nil {
		close(b.blockChan)
	}

	// Wait for handler to finish.
	b.wg.Wait()

	if len(b.fBlocks.blockList) == 0 {
		return fileBlocks{}
	}

	var bf internel.BloomFilter
	if b.opts.BloomFalsePositive > 0 {
		bits := internel.BloomBitsPerKey(len(b.blkBuilder.keyHashes), b.opts.BloomFalsePositive)
		bf = internel.NewBloomFilter(b.blkBuilder.keyHashes, bits)
	}

	index, dataSize := b.buildIndex(bf)
	checksum := b.blkBuilder.calculateCheckSum(index)
	old := b.fBlocks
	b.fBlocks = &fileBlocks{}

	old.checksum = checksum
	old.index = index
	old.size = int(dataSize) + len(index) + len(checksum) + 4 + 4

	return *old
}

func (b *TableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	n := len(b.fBlocks.blockList)
	tableIndex := tableIndex{
		baseKeys:     make([][]byte, n),
		blockOffsets: make([]uint32, n),
		blockLens:    make([]uint32, n),
	}

	var offset uint32
	for i, blk := range b.fBlocks.blockList {
		tableIndex.baseKeys[i] = blk.baseKey
		tableIndex.blockOffsets[i] = offset
		tableIndex.blockLens[i] = uint32(blk.end)

		offset += uint32(blk.end)
	}

	tableIndex.bloom = bloom
	tableIndex.onDiskSize = b.blkBuilder.onDiskSize
	tableIndex.maxVersion = b.blkBuilder.maxVersion
	tableIndex.uncompressedSize = b.blkBuilder.uncompressedSize
	tableIndex.keyCount = uint32(len(b.blkBuilder.keyHashes))

	return tableIndex.Encode(), offset
}
