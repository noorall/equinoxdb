package storage

import (
	"equinox/internel"
	"go.uber.org/zap"
	"os"
	"time"
)

func (e *Engine) flushMemTable(c *internel.Closer) {
	defer c.Done()

	for {
		mt := e.mm.TakeFlushMemTable()
		if mt == nil {
			e.logger.Warn("Flush: take a empty memTable, stop!")
			return
		}
		for {
			newFiles, err := e.compactor.WriteSnapshot(mt.Cache, e.logger)
			if err != nil {
				e.logger.Warn("Error writing memTable from compactor, retrying", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			e.Lock()
			err = e.filestore.Replace(nil, newFiles)
			e.Unlock()
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
				break
			}
		}
	}
}
