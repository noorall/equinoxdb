package store

type BatchDeleter interface {
	DeleteRange(keys [][]byte, min, max int64) error
	Commit() error
	Rollback() error
}

type batchDelete struct {
	r *TSMReader
}

func (b *batchDelete) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	// If the keys can't exist in this TSM file, skip it.
	minKey, maxKey := keys[0], keys[len(keys)-1]
	if !b.r.index.OverlapsKeyRange(minKey, maxKey) {
		return nil
	}

	// If the timerange can't exist in this TSM file, skip it.
	if !b.r.index.OverlapsTimeRange(minTime, maxTime) {
		return nil
	}

	if err := b.r.tombstoner.AddRange(keys, minTime, maxTime); err != nil {
		return err
	}

	return nil
}

func (b *batchDelete) Commit() error {
	defer b.r.deleteMu.Unlock()
	if err := b.r.tombstoner.Flush(); err != nil {
		return err
	}

	return b.r.applyTombstones()
}

func (b *batchDelete) Rollback() error {
	defer b.r.deleteMu.Unlock()
	return b.r.tombstoner.Rollback()
}

type BatchDeleters []BatchDeleter

func (a BatchDeleters) DeleteRange(keys [][]byte, min, max int64) error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.DeleteRange(keys, min, max) }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

func (a BatchDeleters) Commit() error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.Commit() }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

func (a BatchDeleters) Rollback() error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.Rollback() }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}
