package equinox

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"equinox/internel"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"fmt"
)

type Manifest struct {
	Levels []levelsManifest
	Tables map[uint64]tableManifest

	creations int
	deletions int
}

type tableManifest struct {
	level       uint8
	compression CompressionType
}

type levelsManifest struct {
	Tables map[uint64]struct{} // tableid set
}

func NewManifest() Manifest {
	return Manifest{
		Levels: make([]levelsManifest, 0),
		Tables: make(map[uint64]tableManifest),
	}
}

func (m *Manifest) toChanges() manifestChanges {
	changes := []*manifestChange{}
	for tid, tm := range m.Tables {
		changes = append(changes,
			newManifestChange(mcreate, tid, uint64(tm.level), tm.compression))
	}

	return manifestChanges(changes)
}

func (m *Manifest) clone() Manifest {
	res := NewManifest()
	// just copy, no errs
	_ = applyManifestChanges(&res, m.toChanges())

	return res
}

type manifestFile struct {
	dir                       string
	deletionsRewriteThreshold int

	manifest Manifest

	lock sync.Mutex
	fd   *os.File
}

const (
	ManifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 10000
	manifestDeletionsRatio            = 10
)

// OpenManifestFile open the manifest files if exists, or create a new one if non.
func OpenManifestFile(dir string) (*manifestFile, Manifest, error) {
	filename := filepath.Join(dir, ManifestFilename)
	ef, err := openExistingFile(filename)
	if err != nil {
		// check if the file exists
		if !os.IsNotExist(err) {
			return nil, Manifest{}, err
		}

		// so the file is not exists, need create a new one.
		m := NewManifest()
		nf, _, err := rewrite(dir, &m)
		if err != nil {
			return nil, Manifest{}, err
		}

		mf := &manifestFile{
			dir:                       dir,
			deletionsRewriteThreshold: manifestDeletionsRewriteThreshold,
			manifest:                  m.clone(),
			fd:                        nf,
		}

		return mf, m, nil
	}

	// already exists a manifest file, read it and construct a manifest.
	manifest, offset, err := ReplyManifestFile(ef)
	if err != nil {
		_ = ef.Close()
		return nil, Manifest{}, err
	}

	// Truncate file, if there is any broken changes in file will dropped.
	if err := ef.Truncate(offset); err != nil {
		_ = ef.Close()
		return nil, Manifest{}, err
	}

	// seek to the end of file, prepare to write new changes
	if _, err = ef.Seek(0, io.SeekEnd); err != nil {
		_ = ef.Close()
		return nil, Manifest{}, err
	}

	mf := &manifestFile{
		dir:                       dir,
		deletionsRewriteThreshold: manifestDeletionsRewriteThreshold,
		manifest:                  manifest.clone(),
		fd:                        ef,
	}

	return mf, manifest, nil
}

func (mf *manifestFile) AddManifestChange(changes manifestChanges) error {
	buf := changes.encode()

	mf.lock.Lock()
	if err := applyManifestChanges(&mf.manifest, changes); err != nil {
		mf.lock.Unlock()
		return err
	}

	// if there are too many deletion change, a new manifest should be rewrite.
	// that can keep the size of manifest file within a reasonble range.
	if mf.manifest.deletions > mf.deletionsRewriteThreshold &&
		mf.manifest.deletions > manifestDeletionsRatio*(mf.manifest.creations-mf.manifest.deletions) {
		if err := mf.rewrite(); err != nil {
			mf.lock.Unlock()
			return err
		}
	} else {
		// add length and checksum
		lenCrcBuf := writeLengthAndChecksum(buf)
		buf = append(lenCrcBuf, buf...)

		// write
		if _, err := mf.fd.Write(buf); err != nil {
			mf.lock.Unlock()
			return err
		}
	}

	mf.lock.Unlock()
	// the sync may be slow, so unlock before
	return mf.fd.Sync()
}

func ReplyManifestFile(mf *os.File) (Manifest, int64, error) {
	reader := bufio.NewReader(mf)
	if err := readHeader(reader); err != nil {
		return Manifest{}, 0, err
	}

	stat, err := mf.Stat()
	if err != nil {
		return Manifest{}, 0, err
	}

	build := NewManifest()
	offset := uint32(8)
	// keep read util to the end of file or broken record
	for {
		length, checksum, err := readLengthAndChecksum(reader)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			return Manifest{}, 0, err
		}

		offset += 8
		// check if file corrupted.
		if length > uint32(stat.Size()) {
			return Manifest{}, 0, fmt.Errorf(
				"Manifest changes' length: %d greater than file size: %d. Manifest file might be corrupted",
				length, stat.Size())
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(reader, buf); err != nil {
			// dont read the broken changes
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			return Manifest{}, 0, err
		}

		// verify checksum
		if crc32.Checksum(buf, internel.CastagnoliCrcTable) != internel.BytesToU32(checksum) {
			return Manifest{}, 0, ErrChecksumMismatch
		}
		mchanges := manifestChanges{}
		mchanges.decode(buf)

		// apply changes
		if err := applyManifestChanges(&build, mchanges); err != nil {
			return Manifest{}, 0, err
		}

		offset += length
	}

	return build, int64(offset), nil
}

func (mf *manifestFile) Close() error {
	return mf.fd.Close()
}

// Has to be 4 bytes.  The value can never change, ever, anyway.
var magicText = [4]byte{'G', 'O', 'B', 'T'}

const magicVersion = 1

func (mf *manifestFile) rewrite() error {
	// close prev manifest file
	if err := mf.Close(); err != nil {
		return err
	}

	nf, creations, err := rewrite(mf.dir, &mf.manifest)
	if err != nil {
		return err
	}

	mf.fd = nf // replace with new file
	mf.manifest.creations = creations
	mf.manifest.deletions = 0 // reset

	return nil
}

func rewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewriteFile := filepath.Join(dir, manifestRewriteFilename)
	nf, err := openFileWithTrunc(rewriteFile)
	if err != nil {
		return nil, 0, err
	}

	buf := writeHeader()
	changeBuf := m.toChanges().encode()

	buf = append(buf, writeLengthAndChecksum(changeBuf)...)
	buf = append(buf, changeBuf...)
	if _, err := nf.Write(buf); err != nil {
		nf.Close()
		return nil, 0, err
	}

	if err := nf.Sync(); err != nil {
		nf.Close()
		return nil, 0, err
	}

	if err = nf.Close(); err != nil {
		return nil, 0, err
	}

	// rename rewrite file to manifest file
	manifestFile := filepath.Join(dir, ManifestFilename)
	if err := os.Rename(rewriteFile, manifestFile); err != nil {
		return nil, 0, err
	}

	// open the manifest file
	nf, err = openExistingFile(manifestFile)
	if err != nil {
		return nil, 0, err
	}
	// seek to end, prepare to next append
	if _, err := nf.Seek(0, io.SeekEnd); err != nil {
		nf.Close()
		return nil, 0, err
	}

	// sync directory that the manifest file inside, make the directory infos sync.
	// So the new file will be visible (if the system crashes).
	if err := syncDir(dir); err != nil {
		nf.Close()
		return nil, 0, err
	}

	return nf, len(m.Tables), nil
}

// always write begin of manifest file
func writeHeader() []byte {
	buf := make([]byte, 8)
	copy(buf[:4], magicText[:])
	binary.BigEndian.PutUint32(buf[4:], magicVersion)

	return buf
}

func readHeader(reader *bufio.Reader) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return ErrBadMagic
	}

	if !bytes.Equal(buf[:4], magicText[:]) {
		return ErrBadMagic
	}

	version := binary.BigEndian.Uint32(buf[4:])
	if version != magicVersion {
		return Wrapf(ErrMFUnsupportedVersion, "acutal version: %d (but support version %d).\n",
			version, magicVersion)
	}

	return nil
}

func writeLengthAndChecksum(changeBuf []byte) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(buf[4:], crc32.Checksum(changeBuf, internel.CastagnoliCrcTable))

	return buf
}

func readLengthAndChecksum(reader *bufio.Reader) (uint32, []byte, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return 0, nil, err
	}

	len := binary.BigEndian.Uint32(buf[:4])
	checksum := buf[4:]

	return len, checksum, nil
}

func applyManifestChange(m *Manifest, change *manifestChange) error {
	switch change.op {
	case mcreate:
		if _, ok := m.Tables[change.tableId]; ok {
			return fmt.Errorf("MANIFEST Invalid, table %d exists.", change.tableId)
		}
		m.Tables[change.tableId] = tableManifest{
			level:       uint8(change.levelId),
			compression: change.compression,
		}

		for len(m.Levels) <= int(change.levelId) {
			m.Levels = append(m.Levels, levelsManifest{Tables: map[uint64]struct{}{}})
		}

		m.Levels[change.levelId].Tables[change.tableId] = struct{}{}
		m.creations++
		return nil
	case mdelete:
		tm, ok := m.Tables[change.tableId]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d.", change.tableId)
		}

		delete(m.Tables, change.tableId)
		delete(m.Levels[tm.level].Tables, change.tableId)
		m.deletions++
		return nil
	default:
		return fmt.Errorf("MANIFEST has invalid operation %d.", change.op)
	}
}

func applyManifestChanges(m *Manifest, changes manifestChanges) error {
	for _, change := range []*manifestChange(changes) {
		if err := applyManifestChange(m, change); err != nil {
			return err
		}
	}

	return nil
}

type manifestOperation uint32

const (
	mcreate manifestOperation = iota
	mdelete
)

const changeSize = uint32(unsafe.Sizeof(manifestChange{}))

type manifestChange struct {
	tableId     uint64
	levelId     uint64
	op          manifestOperation
	compression CompressionType
}

func newManifestChange(op manifestOperation, tid uint64,
	lid uint64, compress CompressionType) *manifestChange {
	return &manifestChange{
		tableId:     tid,
		levelId:     lid,
		op:          op,
		compression: compress,
	}
}

func (m *manifestChange) encode(buf []byte) uint32 {
	var written uint32
	binary.BigEndian.PutUint32(buf[written:], uint32(m.op))
	written += 4
	binary.BigEndian.PutUint32(buf[written:], uint32(m.compression))
	written += 4
	binary.BigEndian.PutUint64(buf[written:], m.tableId)
	written += 8
	binary.BigEndian.PutUint64(buf[written:], m.levelId)
	written += 8

	return written
}

func (m *manifestChange) decode(data []byte) int {
	read := 0
	m.op = manifestOperation(binary.BigEndian.Uint32(data[read:]))
	read += 4

	m.compression = CompressionType(binary.BigEndian.Uint32(data[read:]))
	read += 4

	m.tableId = binary.BigEndian.Uint64(data[read:])
	read += 8
	m.levelId = binary.BigEndian.Uint64(data[read:])

	return read + 8
}

type manifestChanges []*manifestChange

func (m manifestChanges) encode() []byte {
	changes := uint32(len(m))
	var written uint32
	buf := make([]byte, changes*changeSize)

	// write changes
	for _, mc := range m {
		written += mc.encode(buf[written:])
	}

	return buf[:written]
}

func (m *manifestChanges) decode(buf []byte) {
	for offset := 0; offset < len(buf); {
		change := &manifestChange{}
		offset += change.decode(buf[offset:])
		*m = append(*m, change)
	}
}

func openFileWithTrunc(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func openExistingFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR, 0)
}

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return Wrapf(err, "While opening directory: %s.", dir)
	}

	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return Wrapf(err, "While syncing directory: %s.", dir)
	}
	return Wrapf(closeErr, "While closing directory: %s.", dir)
}
