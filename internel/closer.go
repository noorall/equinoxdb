package internel

import (
	"context"
	"sync"
)

// Closer holds the two things we need to close a goroutine and wait for it to
// finish: a chan to tell the goroutine to shut down, and a WaitGroup with
// which to wait for it to finish shutting down.
type Closer struct {
	wait sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCloser constructs a new Closer, with an initial count on the WaitGroup.
func NewCloser(init int) *Closer {
	closer := &Closer{}
	closer.wait.Add(init)

	closer.ctx, closer.cancel = context.WithCancel(context.Background())
	return closer
}

// AddRunning Add()'s delta to the WaitGroup.
func (c *Closer) AddRunning(delta int) {
	c.wait.Add(delta)
}

// Ctx can be used to get a context,
// which would automatically get cancelled when Signal is called.
func (c *Closer) Ctx() context.Context {
	if c == nil {
		return context.Background()
	}
	return c.ctx
}

// Signal signals the HasBeenClosed signal.
func (c *Closer) Signal() {
	c.cancel()
}

var emptyCloserChan <-chan struct{}

// HasBeenClosed gets signaled when Signal() is called.
func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil {
		return emptyCloserChan
	}

	return c.ctx.Done()
}

// Done calls Done() on the WaitGroup.
func (c *Closer) Done() {
	if c == nil {
		return
	}

	c.wait.Done()
}

// Wait waits on the WaitGroup. (It waits for NewCloser's initial value, AddRunning, and Done
// calls to balance out.)
func (c *Closer) Wait() {
	c.wait.Wait()
}

// SignalAndWait calls Signal(), then Wait().
func (c *Closer) SignalAndWait() {
	c.Signal()
	c.Wait()
}
