package workerpool

import "sync"

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

func (eg *ErrorGroup) Wait() error {
	eg.g.Wait()
	// TODO: return a union error
	if len(eg.errs) > 0 {
		return eg.errs[0]
	}
	return nil
}

func (eg *ErrorGroup) Done(err error) {
	eg.m.Lock()
	defer eg.m.Unlock()
	if err != nil {
		eg.errs = append(eg.errs, err)
	}
	eg.g.Done()
}
