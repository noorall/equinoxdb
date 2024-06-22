package storage

import (
	"bytes"
	"equinox/pkg/models"
	"equinox/storage/types"
	equinox "equinox/types"
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
	timeBytes              = []byte("time")
	keyFieldSeparatorBytes = []byte(keyFieldSeparator)
	emptyBytes             = []byte{}
)

type Engine struct {
	sync.RWMutex
	mm *MemManager

	option *equinox.Options

	syncWrite bool

	logger *zap.Logger
}

func NewEngine(option *equinox.Options) (*Engine, error) {
	logger := zap.NewNop()
	mm, err := NewMemManager(option, logger)
	if err != nil {
		return nil, err
	}
	return &Engine{
		mm:        mm,
		option:    option,
		syncWrite: option.Sync,
		logger:    logger,
	}, nil
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
	for err = e.mm.EnsureMemForWrite(); errors.Is(err, ErrNoRoom); err = e.mm.EnsureMemForWrite() {
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
