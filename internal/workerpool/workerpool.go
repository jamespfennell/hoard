package workerpool

import (
	"github.com/jamespfennell/hoard/internal/util"
	"sync"
)

// TODO: move to util
type WorkerPool struct {
	c chan func()
}

func (pool *WorkerPool) Run(f func()) {
	pool.c <- f
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	pool := WorkerPool{
		c: make(chan func()),
	}
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				f := <-pool.c
				f()
			}
		}()
	}
	return &pool
}

type ErrorGroup struct {
	g    sync.WaitGroup
	m    sync.Mutex
	errs []error
}

func (eg *ErrorGroup) Add(delta int) {
	eg.g.Add(delta)
}

func (eg *ErrorGroup) Done(err error) {
	eg.m.Lock()
	defer eg.m.Unlock()
	if err != nil {
		eg.errs = append(eg.errs, err)
	}
	eg.g.Done()
}

func (eg *ErrorGroup) Wait() error {
	eg.g.Wait()
	return util.NewMultipleError(eg.errs...)
}
