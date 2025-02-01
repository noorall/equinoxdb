package storage

import (
	"equinox/storage/compactor"
	"equinox/storage/memory"
	"equinox/storage/metric"
	"github.com/prometheus/client_golang/prometheus"
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
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for {
		e.mu.RLock()
		quit := e.snapDone
		e.mu.RUnlock()
		select {
		case <-quit:
			return
		default:
			mt := e.mm.TakeFlushMemTable()
			if mt == nil {
				return
			}
			e.doCompactMemTable(mt)
		}
	}
}

func (e *Engine) doCompactMemTable(mt *memory.MemTable) {
	started := time.Now()
	for {
		e.mu.RLock()
		quit := e.snapDone
		e.mu.RUnlock()
		select {
		case <-quit:
			return
		default:
			mt.Cache.Deduplicate()
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
				if err == compactor.ErrCompactionsDisabled {
					return
				}
				e.compactionStats.Failed.With(prometheus.Labels{metric.LevelKey: metric.LevelCache}).Inc()
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
				elapsed := time.Since(started)
				e.compactionStats.Duration.With(prometheus.Labels{metric.LevelKey: metric.LevelCache}).Observe(elapsed.Seconds())
				if err == nil {
					e.logger.Info("Cache for path written", zap.String("path", e.option.Dir), zap.Duration("duration", elapsed))
				}
				return
			}
		}
	}
}
