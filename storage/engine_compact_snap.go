package storage

import (
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

func (e *Engine) enableSnapshotCompactions() {
	// Check if already enabled under read lock
	e.mu.RLock()
	if e.snapDone != nil {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()

	// Check again under write lock
	e.mu.Lock()
	if e.snapDone != nil {
		e.mu.Unlock()
		return
	}

	e.compactor.EnableSnapshots()
	e.snapDone = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	e.snapWG = wg
	e.mu.Unlock()

	go func() { defer wg.Done(); e.compactMemTable() }()
}

func (e *Engine) disableSnapshotCompactions() {
	e.mu.Lock()
	if e.snapDone == nil {
		e.mu.Unlock()
		return
	}

	// We may be in the process of stopping snapshots.  See if the channel
	// was closed.
	select {
	case <-e.snapDone:
		e.mu.Unlock()
		return
	default:
	}

	// close flush chain
	e.mm.Close()

	close(e.snapDone)
	e.compactor.DisableSnapshots()
	wg := e.snapWG
	e.mu.Unlock()

	// Wait for the snapshot goroutine to exit.
	wg.Wait()

	// Signal that the goroutines are exit and everything is stopped by setting
	// snapDone to nil.
	e.mu.Lock()
	e.snapDone = nil
	e.mu.Unlock()
}

func (e *Engine) compactMemTable() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		e.mu.RLock()
		quit := e.snapDone
		e.mu.RUnlock()
		select {
		case <-quit:
			return
		case <-t.C:
			e.doCompactMemTable()
		}
	}
}

func (e *Engine) doCompactMemTable() {
	mt := e.mm.TakeFlushMemTable()
	if mt == nil {
		e.logger.Warn("Flush: take a empty memTable")
		return
	}
	for {
		e.mu.RLock()
		quit := e.snapDone
		e.mu.RUnlock()
		select {
		case <-quit:
			return
		default:
			newFiles, err := e.compactor.WriteSnapshot(mt.Cache, e.logger)
			if err != nil {
				e.logger.Warn("Error writing memTable from compactor, retrying", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			e.mu.Lock()
			err = e.filestore.Replace(nil, newFiles)
			e.mu.Unlock()
			if err != nil {
				e.logger.Warn("Error adding new files. Removing temp files.", zap.Error(err))
				// Remove the new snapshot files. We will try again.
				for _, file := range newFiles {
					err = os.Remove(file)
					if err != nil {
						e.logger.Warn("Unable to remove file", zap.String("path", file), zap.Error(err))
					}
				}
			} else {
				e.mm.OnMemTableFlushed(mt)
				return
			}
		}
	}
}
