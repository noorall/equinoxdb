package storage

import (
	"bytes"
	"context"
	"equinox/pkg/limiter"
	"equinox/pkg/models"
	"equinox/storage/compactor"
	"equinox/storage/config"
	"equinox/storage/memory"
	"equinox/storage/store"
	"equinox/storage/types"
	"errors"
	"fmt"
	"go.uber.org/zap"
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

	syncWrite bool

	option config.Option

	logger *zap.Logger
}

func NewEngine(opt config.Option) (*Engine, error) {
	logger := zap.NewNop()
	mm, err := memory.NewMemManager(opt, logger)
	if err != nil {
		return nil, err
	}

	// TODO: optimize this part
	fs := store.NewFileStore(opt.Dir)
	fs.OpenLimiter = opt.OpenLimiter

	c := compactor.NewCompactor()
	c.Dir = opt.Dir
	c.FileStore = fs
	c.RateLimit = opt.CompactionThroughputLimiter
	c.VFileManager = make(map[int]*store.VFileManager)

	planner := compactor.NewDefaultPlanner(fs, opt.CompactFullWriteColdDuration)
	activeCompactions := &compactionCounter{}
	e := &Engine{
		mm:                mm,
		option:            opt,
		syncWrite:         opt.SyncWrite,
		logger:            logger,
		filestore:         fs,
		compactor:         c,
		compactionPlan:    planner,
		compactionLimiter: opt.CompactionLimiter,
		activeCompactions: activeCompactions,
		scheduler:         newScheduler(activeCompactions, opt.CompactionLimiter.Capacity()),
	}

	return e, nil
}
func (e *Engine) Open(ctx context.Context) error {
	if err := e.filestore.Open(ctx); err != nil {
		return err
	}
	vFileManager, err := store.NewVFileManager(e.option, models.Default)
	if err != nil {
		return err
	}
	e.compactor.VFileManager[models.Default] = vFileManager
	e.compactor.Open()
	e.SetCompactionsEnabled(true)
	return nil
}
func (e *Engine) Close() error {
	time.Sleep(5 * time.Second)
	e.SetCompactionsEnabled(false)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.compactionDone = nil

	err := e.filestore.Close()

	return err
}

func (e *Engine) WritePoints(points []models.Point) error {
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
			case models.Float:
				fv, err := iter.FloatValue()
				if err != nil {
					return err
				}
				v = types.NewFloatValue(t, fv)
			case models.Integer:
				iv, err := iter.IntegerValue()
				if err != nil {
					return err
				}
				v = types.NewIntegerValue(t, iv)
			case models.Unsigned:
				iv, err := iter.UnsignedValue()
				if err != nil {
					return err
				}
				v = types.NewUnsignedValue(t, iv)
			case models.String:
				v = types.NewStringValue(t, iter.StringValue())
			case models.Boolean:
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
			e.logger.Debug("Making room for writes.")
		}

		time.Sleep(10 * time.Millisecond)
	}
	return e.mm.WriteMulti(values, e.syncWrite)
}

func (e *Engine) DeleteSeriesRange(itr models.SeriesIterator, min, max int64) error {
	return nil
}

func (e *Engine) LastModified() time.Time {
	return e.filestore.LastModified()
}
