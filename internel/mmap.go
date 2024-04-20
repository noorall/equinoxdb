package internel

import (
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const RW_ = 0666

type MmapFile struct {
	Data    []byte
	Fd      *os.File
	NewFile bool // if a file is created when it open, this is true
}

func OpenMmapFile(filename string, flag int, max int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, RW_)

	if err != nil {
		return nil, err
	}

	return OpenMmapFileWithFD(fd, flag, max)
}

func OpenMmapFileWithFD(fd *os.File, flag int, max int) (*MmapFile, error) {
	stat, err := fd.Stat()

	if err != nil {
		return nil, err
	}

	size := stat.Size()
	newFile := false
	if max > 0 && size == 0 {
		// If file is empty, truncate it to sz.
		if err := fd.Truncate(int64(max)); err != nil {
			return nil, err
		}

		size = int64(max)
		newFile = true
	}

	prod := unix.PROT_READ | unix.PROT_WRITE
	if flag == os.O_RDONLY {
		prod = unix.PROT_READ
	}

	buf, err := unix.Mmap(int(fd.Fd()), 0, int(size), prod, unix.MAP_SHARED)

	if err != nil {
		return nil, err
	}

	if size == 0 {
		dir, _ := filepath.Split(fd.Name())

		go dsync(dir)
	}

	return &MmapFile{
		Data:    buf,
		Fd:      fd,
		NewFile: newFile,
	}, nil
}

type reader struct {
	data   []byte
	offset int
}

func (r *reader) Read(buf []byte) (int, error) {
	if r.offset > len(r.data) {
		return 0, io.EOF
	}

	n := copy(buf, r.data[r.offset:])
	r.offset += n
	if n < len(buf) {
		return n, io.EOF
	}

	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &reader{data: m.Data, offset: offset}
}

func (m *MmapFile) Truncate(size int64) error {
	if m.Fd == nil {
		return nil
	}

	if err := m.Sync(); err != nil {
		return err
	}

	if err := unix.Munmap(m.Data); err != nil {
		return err
	}

	if err := m.Fd.Truncate(size); err != nil {
		return err
	}

	var err error
	m.Data, err = unix.Mmap(int(m.Fd.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	return err
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := unix.Munmap(m.Data); err != nil {
		return err
	}

	m.Data = nil

	if err := m.Fd.Truncate(0); err != nil {
		return err
	}

	if err := m.Fd.Close(); err != nil {
		return err
	}

	return os.Remove(m.Fd.Name())
}

func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}

	if err := m.Sync(); err != nil {
		return err
	}

	if err := unix.Munmap(m.Data); err != nil {
		return err
	}

	return m.Fd.Close()
}

func (m *MmapFile) CloseWithTruncate(size int64) error {
	if m.Fd == nil {
		return nil
	}

	if err := m.Sync(); err != nil {
		return err
	}

	if err := unix.Munmap(m.Data); err != nil {
		return err
	}

	if size >= 0 {
		if err := m.Fd.Truncate(size); err != nil {
			return err
		}
	}

	return m.Fd.Close()
}

func (m *MmapFile) Sync() error {
	if m.Fd == nil {
		return nil
	}

	return unix.Msync(m.Data, unix.MS_SYNC)
}

func dsync(dirName string) error {
	d, err := os.Open(dirName)

	if err != nil {
		return err
	}

	if err := d.Sync(); err != nil {
		return err
	}

	if err := d.Close(); err != nil {
		return err
	}

	return nil
}
