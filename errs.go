package equinox

import (
	"errors"
	"fmt"
	"log"
)

var (
	ErrTruncating    = errors.New("do truncate")
	ErrStopIteration = errors.New("stop iteration")
	ErrBadWAL        = errors.New(
		"WAL log is broken, need to be truncated that might cause data loss")
	ErrChecksumMismatch     = errors.New("checksum mismatch")
	ErrCompressionType      = errors.New("unsupported compression type")
	ErrBadMagic             = errors.New("manifest has bad magic")
	ErrMFUnsupportedVersion = errors.New("manifest has unsupported version")
	ErrFillTable            = errors.New("unable to fill tables")
	ErrNoRewrite            = errors.New("value log GC attempt didn't result in any cleanup")
	ErrRejected             = errors.New("value log GC request rejected")
	ErrValueLogSize         = errors.New("invalid ValueLogFileSize, must be in range [1MB, 2GB)")
	ErrNoRoom               = errors.New("no room for write")
	ErrDBClosed             = errors.New("DB Closed")
	ErrBatchTooBig          = errors.New("batch is too big to fit into one batch write")
	ErrKeyNotFound          = errors.New("key not found")
	ErrEmptyKey             = errors.New("key cannot be empty")
	ErrEmptyBatch           = errors.New("batch cannot be empty")
	ErrBlockedWrites        = errors.New("writes are blocked, possibly due to DropAll or Close")
)

func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func Check2(_ interface{}, err error) {
	Check(err)
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf("assert failed"))
	}
}

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf(format, args...))
	}
}

func Wrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s error: %+v", msg, err)
}

func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(format+" error: %+v", append(args, err)...)
}
