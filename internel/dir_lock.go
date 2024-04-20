package internel

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// dirLockGuard holds a lock on a directory and a pid file inside.
type DirLockGuard struct {
	// File handle on the directory, which should be locked.
	f *os.File
	// The absolute path to the pid file.
	path string
}

// AcquireDirLock gets a lock on the directory (using flock).
// It will also write our pid to dir/pidFileName for convenience.
func AcquireDirLock(dir string, pidFileName string) (*DirLockGuard, error) {
	pid, err := filepath.Abs(filepath.Join(dir, pidFileName))
	if err != nil {
		return nil, err
	}

	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	opts := unix.LOCK_EX | unix.LOCK_NB // exclusive other processes without blocking
	err = unix.Flock(int(f.Fd()), opts)
	if err != nil {
		f.Close()
		return nil, err
	}

	err = ioutil.WriteFile(pid, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0666)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &DirLockGuard{f: f, path: pid}, nil
}

// Release deletes the pid file and releases lock on the directory.
func (g *DirLockGuard) Release() error {
	err := os.Remove(g.path)
	if closeErr := g.f.Close(); err == nil {
		err = closeErr
	}

	g.f = nil
	g.path = ""

	return err
}
