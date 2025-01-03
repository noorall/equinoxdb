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
	"equinox/storage/metric"
	separator "equinox/storage/separator"
	"equinox/storage/store"
	"equinox/storage/types"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
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
	separateDecider   *separator.SeparateDecider

	filestore *store.FileStore

	vFileRegionManager *store.VFileRegionManager
	vFileGcDone        chan struct{}
	vFileGcWG          *sync.WaitGroup
	syncWrite          bool

	compactionStats *metric.CompactionMetrics
	engineStats     *metric.EngineMetrics

	option config.Option
	logger *zap.Logger
}

func NewEngine(opt config.Option) (*Engine, error) {
	labs := metric.GetEngineLabs(opt)
	logger, _ := zap.NewProduction()
	logger = logger.With(zap.String("engine", "equinox"))
	mm, err := memory.NewMemManager(opt, logger)
	if err != nil {
		return nil, err
	}

	// TODO: optimize this part
	vFileRegionManager := store.NewVFileRegionManager(opt)

	fs := store.NewFileStore(opt.Dir)
	fs.OpenLimiter = opt.OpenLimiter
	fs.VM = vFileRegionManager
	fs.Metric = metric.NewFileStoreMetrics(labs)

	var separateDecider *separator.SeparateDecider

	if opt.SeparateDeciderEnabled {
		separateDecider = separator.NewSeparateDecider(opt.SeparateDistMu, opt.SeparateDistSigma, opt.SeparateFactor, opt.SeparateZetaStep, opt.SeparateThreshold)
	}

	c := compactor.NewCompactor()
	c.Dir = opt.Dir
	c.FileStore = fs
	c.RateLimit = opt.CompactionThroughputLimiter
	c.VM = vFileRegionManager
	c.SeparateDecider = separateDecider
	c.SeparateThreshold = opt.SeparateThreshold
	c.SeparateEnabled = opt.SeparateEnabled
	c.Stats = metric.NewCompactionMetrics(labs)

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
		separateDecider:    separateDecider,
		compactionStats:    c.Stats,
		engineStats:        metric.NewEngineMetrics(labs),
	}

	return e, nil
}
func (e *Engine) Open(ctx context.Context) error {
	e.logger.Info("starting create filestore")
	if err := e.filestore.Open(ctx); err != nil {
		return err
	}
	e.logger.Info("starting create value file manager")
	err := e.vFileRegionManager.RegisterRegion(models.Default)
	if err != nil {
		return err
	}
	e.logger.Info("starting compaction worker")
	e.compactor.Open()
	e.SetCompactionsEnabled(true)
	e.logger.Info("starting value file gc worker")
	e.enableValueFileGc()
	if e.option.EnableMetrics {
		metric.RunMetricServer(e.logger, e.option.MetricPort)
	}
	return nil
}

func (e *Engine) Close() error {
	e.logger.Info("starting close memory manager")
	e.mm.Close()

	e.logger.Info("starting close compaction worker")
	e.SetCompactionsEnabled(false)

	e.logger.Info("starting close value file gc worker")
	e.disableValueFileGc()

	e.mu.Lock()
	defer e.mu.Unlock()
	e.compactionDone = nil
	e.vFileGcDone = nil

	e.logger.Info("starting close filestore")
	err := e.filestore.Close()

	e.logger.Info("starting value file region manager")
	err = e.vFileRegionManager.Close()

	return err
}

func (e *Engine) Write(point types.Point) error {
	return e.WriteBatch([]types.Point{point})
}

func (e *Engine) WriteBatch(points []types.Point) error {
	start := time.Now()
	values := make(map[string][]types.Value, len(points))
	var (
		keyBuf  []byte
		baseLen int
	)
	size := 0
	prev := int64(-1)
	for _, p := range points {
		keyBuf = append(keyBuf[:0], p.Key()...)
		keyBuf = append(keyBuf, keyFieldSeparator...)
		baseLen = len(keyBuf)
		iter := p.FieldIterator()
		t := p.Time().UnixNano()
		if e.separateDecider != nil {
			e.separateDecider.UpdateAvgTd(float64(t - start.UnixNano()))
			if prev != -1 {
				e.separateDecider.UpdateAvgTg(float64(t - prev))
			}
		}
		prev = t
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
			size = size + v.Size() + len(keyBuf)
			values[string(keyBuf)] = append(values[string(keyBuf)], v)
		}
	}
	e.engineStats.WrittenDelay.With(prometheus.Labels{"type": "process"}).Observe(float64(time.Since(start).Milliseconds()))
	t2 := time.Now()

	var err error
	i := 0
	for err = e.mm.EnsureMemForWrite(); errors.Is(err, memory.ErrNoRoom); err = e.mm.EnsureMemForWrite() {
		i++

		if i%100 == 0 {
			e.logger.Info("Waiting room for writes.")
		}

		time.Sleep(10 * time.Millisecond)
	}
	e.engineStats.WrittenDelay.With(prometheus.Labels{"type": "waiting"}).Observe(float64(time.Since(t2).Milliseconds()))
	t2 = time.Now()

	err = e.mm.WriteMulti(values, e.syncWrite)

	e.engineStats.WrittenDelay.With(prometheus.Labels{"type": "writing"}).Observe(float64(time.Since(t2).Milliseconds()))

	e.engineStats.WrittenDelay.With(prometheus.Labels{"type": "total"}).Observe(float64(time.Since(start).Milliseconds()))

	val := float64(size) / float64(1024) / float64(1024) / time.Since(start).Seconds()
	e.engineStats.WrittenOutput.Set(val)
	return err
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

	e.disableLevelCompactions(true)
	e.disableValueFileGc()
	defer e.enableLevelCompactions(true)
	defer e.enableValueFileGc()

	_ = e.filestore.Apply(context.Background(), func(r store.SSTFile) error {
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
	if err := e.filestore.Apply(context.Background(), func(r store.SSTFile) error {
		// See if this sst file contains the keys and time range
		minKey, maxKey := keys[0], keys[len(keys)-1]
		sstMin, sstMax := r.KeyRange()

		sstMin, _ = SeriesAndFieldFromCompositeKey(sstMin)
		sstMax, _ = SeriesAndFieldFromCompositeKey(sstMax)

		overlaps := bytes.Compare(sstMin, maxKey) <= 0 && bytes.Compare(sstMax, minKey) >= 0
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
