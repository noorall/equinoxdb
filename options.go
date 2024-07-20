package equinox

import (
	"equinox/internel"
	"equinox/metric"
)

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
type CompressionType uint32

const (
	// WARNING: DON'T change the existing entries!
	NoCompression CompressionType = iota
	SnappyCompression
	ZSTDCompression
)

var (
	DefaultReadOptions  = ReadOptions{VerifyCheckSum: true, FillCache: false, Snapshot: nil, AllVersion: false}
	DefaultWriteOptions = WriteOptions{Sync: false}
)

const (
	maxValueThreshold = 1024 * 1024 // 1 MB
)

type Comparator = func([]byte, []byte) int

type Option func(*Options)

// Options are params for creating DB object.
// Each option X can be setted with WithX method.
type Options struct {
	Dir             string
	CompressionType CompressionType
	comparator      Comparator // internel use only
	// Logger
	Logger internel.Logger

	NumMemtables  int
	MemTableSize  int
	maxBatchSize  int
	maxBatchCount int

	ValueLogDir        string
	ValueLogFileSize   int
	ValueLogMaxEntries int
	ValueThreshold     int

	BlockSize int
	tableSize uint64

	BloomFalsePositive   float64
	ZSTDCompressionLevel int

	MaxLevels               int
	NumLevelZeroTables      int
	NumLevelZeroTablesStall int
	// see https://github.com/facebook/rocksdb/blob/v3.11/include/rocksdb/options.h#L366-L423
	BaseTableSize       int64
	BaseLevelSize       int64
	LevelSizeMultiplier int
	TableSizeMultiplier int
	NumCompactors       int

	VerifyTableChecksum bool

	Separate bool

	VFileWriteParallelism int

	Metric *metric.Metric
}

type ReadOptions struct {
	VerifyCheckSum bool
	FillCache      bool // current doesn't support
	Snapshot       *Snapshot
	AllVersion     bool // return all version value of key, no matter if it deleted
}

type WriteOptions struct {
	Sync     bool
	Separate bool
}

func DefaultOptions(dir string) Options {
	return Options{
		Dir:         dir,
		ValueLogDir: dir,

		MemTableSize:        64 << 20,  // 64 MB
		BaseTableSize:       64 << 20,  // 2 MB
		BaseLevelSize:       256 << 20, // 10 MB
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 4,
		MaxLevels:           4,

		NumCompactors:           4,
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 15,
		NumMemtables:            5,
		BloomFalsePositive:      0.01,
		BlockSize:               4 * 1024, // 4 KB

		// so 2*ValueLogFileSize won't overflow on 32-bit systems.
		ValueLogFileSize:   1<<30 - 1,
		ValueLogMaxEntries: 10000,
		ValueThreshold:     maxValueThreshold,

		CompressionType:      SnappyCompression,
		ZSTDCompressionLevel: 1,

		comparator:          CompareKeys,
		Logger:              internel.DefaultLogger(internel.INFO),
		VerifyTableChecksum: false,
		Separate:            true,
		Metric:              &metric.Metric{},
	}
}
