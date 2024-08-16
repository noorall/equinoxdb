package storage

import (
	"bytes"
	"context"
	"equinox/pkg/limiter"
	"equinox/pkg/models"
	"equinox/storage/compactor"
	"equinox/storage/config"
	"equinox/storage/cursor"
	"equinox/storage/memory"
	"equinox/storage/store"
	"equinox/storage/types"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"math"
	"sync"
	"time"
)

const (
	keyFieldSeparator = "#!~#"
)

var (
	timeBytes = []byte("time")
)

type Engine struct {
	mu sync.RWMutex

	mm       *memory.MemManager
	snapDone chan struct{}
	snapWG   *sync.WaitGroup

	compactor         *compactor.Compactor
	compactionDone    chan struct{}
	compactionWG      *sync.WaitGroup
	compactionPlan    compactor.CompactionPlanner
	levelWorkers      int
	scheduler         *scheduler
	compactionLimiter limiter.Fixed
	activeCompactions *compactionCounter

	filestore *store.FileStore

	vFileRegionManager *store.VFileRegionManager

	syncWrite bool

	option config.Option

	logger *zap.Logger
}

func NewEngine(opt config.Option) (*Engine, error) {
	logger, _ := zap.NewProduction()
	mm, err := memory.NewMemManager(opt, logger)
	if err != nil {
		return nil, err
	}

	// TODO: optimize this part
	vFileRegionManager := store.NewVFileRegionManager(opt)

	fs := store.NewFileStore(opt.Dir)
	fs.OpenLimiter = opt.OpenLimiter
	fs.VM = vFileRegionManager

	c := compactor.NewCompactor()
	c.Dir = opt.Dir
	c.FileStore = fs
	c.RateLimit = opt.CompactionThroughputLimiter
	c.VM = vFileRegionManager

	planner := compactor.NewDefaultPlanner(fs, opt.CompactFullWriteColdDuration)
	activeCompactions := &compactionCounter{}
	e := &Engine{
		mm:                 mm,
		option:             opt,
		syncWrite:          opt.SyncWrite,
		logger:             logger,
		filestore:          fs,
		compactor:          c,
		compactionPlan:     planner,
		compactionLimiter:  opt.CompactionLimiter,
		activeCompactions:  activeCompactions,
		scheduler:          newScheduler(activeCompactions, opt.CompactionLimiter.Capacity()),
		vFileRegionManager: vFileRegionManager,
	}

	return e, nil
}
func (e *Engine) Open(ctx context.Context) error {
	if err := e.filestore.Open(ctx); err != nil {
		return err
	}
	err := e.vFileRegionManager.RegisterRegion(models.Default)
	if err != nil {
		return err
	}
	e.compactor.Open()
	e.SetCompactionsEnabled(true)
	return nil
}

func (e *Engine) Close() error {
	e.mm.Close()

	e.SetCompactionsEnabled(false)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.compactionDone = nil

	err := e.filestore.Close()

	err = e.vFileRegionManager.Close()

	return err
}

func (e *Engine) Write(point types.Point) error {
	return e.WriteBatch([]types.Point{point})
}

func (e *Engine) WriteBatch(points []types.Point) error {
	values := make(map[string][]types.Value, len(points))
	var (
		keyBuf  []byte
		baseLen int
	)
	for _, p := range points {
		keyBuf = append(keyBuf[:0], p.Key()...)
		keyBuf = append(keyBuf, keyFieldSeparator...)
		baseLen = len(keyBuf)
		iter := p.FieldIterator()
		t := p.Time().UnixNano()
		for iter.Next() {
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}

			keyBuf = append(keyBuf[:baseLen], iter.FieldKey()...)

			var v types.Value
			switch iter.Type() {
			case types.Float:
				fv, err := iter.FloatValue()
				if err != nil {
					return err
				}
				v = types.NewFloatValue(t, fv)
			case types.Integer:
				iv, err := iter.IntegerValue()
				if err != nil {
					return err
				}
				v = types.NewIntegerValue(t, iv)
			case types.Unsigned:
				iv, err := iter.UnsignedValue()
				if err != nil {
					return err
				}
				v = types.NewUnsignedValue(t, iv)
			case types.String:
				v = types.NewStringValue(t, iter.StringValue())
			case types.Boolean:
				bv, err := iter.BooleanValue()
				if err != nil {
					return err
				}
				v = types.NewBooleanValue(t, bv)
			default:
				return fmt.Errorf("unknown field type for %s: %s", string(iter.FieldKey()), p.String())
			}
			values[string(keyBuf)] = append(values[string(keyBuf)], v)
		}
	}

	var err error
	i := 0
	for err = e.mm.EnsureMemForWrite(); errors.Is(err, memory.ErrNoRoom); err = e.mm.EnsureMemForWrite() {
		i++

		if i%100 == 0 {
			e.logger.Info("Waiting room for writes.")
		}

		time.Sleep(10 * time.Millisecond)
	}
	return e.mm.WriteMulti(values, e.syncWrite)
}

func (e *Engine) Delete(key []byte) error {
	return e.DeleteRange(key, math.MinInt64, math.MaxInt64)
}

func (e *Engine) DeleteRange(key []byte, min, max int64) error {
	return e.DeleteRangeBatch([][]byte{key}, min, max)
}

func (e *Engine) DeleteRangeBatch(keys [][]byte, min, max int64) error {
	Sort(keys)

	e.mm.DeleteRange(keys, min, max)

	var overlapsTimeRangeMinMax bool
	var overlapsTimeRangeMinMaxLock sync.Mutex

	_ = e.filestore.Apply(context.Background(), func(r store.TSMFile) error {
		if r.OverlapsTimeRange(min, max) {
			overlapsTimeRangeMinMaxLock.Lock()
			overlapsTimeRangeMinMax = true
			overlapsTimeRangeMinMaxLock.Unlock()
		}
		return nil
	})

	if !overlapsTimeRangeMinMax {
		return nil
	}
	// Run the delete on each sst file in parallel
	if err := e.filestore.Apply(context.Background(), func(r store.TSMFile) error {
		// See if this sst file contains the keys and time range
		minKey, maxKey := keys[0], keys[len(keys)-1]
		tsmMin, tsmMax := r.KeyRange()

		tsmMin, _ = SeriesAndFieldFromCompositeKey(tsmMin)
		tsmMax, _ = SeriesAndFieldFromCompositeKey(tsmMax)

		overlaps := bytes.Compare(tsmMin, maxKey) <= 0 && bytes.Compare(tsmMax, minKey) >= 0
		if !overlaps || !r.OverlapsTimeRange(min, max) {
			return nil
		}

		// Delete each key we find in the file.  We seek to the min key and walk from there.
		batch := r.BatchDelete()
		n := r.KeyCount()
		var j int
		for i := r.Seek(minKey); i < n; i++ {
			indexKey, _ := r.KeyAt(i)
			seriesKey, _ := SeriesAndFieldFromCompositeKey(indexKey)

			for j < len(keys) && bytes.Compare(keys[j], seriesKey) < 0 {
				j++
			}

			if j >= len(keys) {
				break
			}
			if bytes.Equal(keys[j], seriesKey) {
				if err := batch.DeleteRange([][]byte{indexKey}, min, max); err != nil {
					_ = batch.Rollback()
					return err
				}
			}
		}
		return batch.Commit()
	}); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Get(key, field []byte, fieldType types.FieldType, startTime, endTime int64, asc bool) (cursor.Cursor, error) {
	realKey := append(key, keyFieldSeparator...)
	realKey = append(realKey, field...)
	switch fieldType {
	case types.Boolean:
		return e.buildBooleanArrayCursor(realKey, startTime, endTime, asc), nil
	case types.Float:
		return e.buildFloatArrayCursor(realKey, startTime, endTime, asc), nil
	case types.Integer:
		return e.buildIntegerArrayCursor(realKey, startTime, endTime, asc), nil
	case types.String:
		return e.buildStringArrayCursor(realKey, startTime, endTime, asc), nil
	case types.Unsigned:
		return e.buildUnsignedArrayCursor(realKey, startTime, endTime, asc), nil
	default:
		return nil, fmt.Errorf("unknown field type")
	}
}

func (e *Engine) LastModified() time.Time {
	return e.filestore.LastModified()
}

// keyCursor returns a store.KeyCursor for the given key starting at time t.
func (e *Engine) keyCursor(key []byte, t int64, ascending bool) *store.KeyCursor {
	return e.filestore.KeyCursor(context.Background(), key, t, ascending)
}
