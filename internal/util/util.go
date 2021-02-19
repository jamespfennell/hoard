package util

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func WithSystemInterrupt(ctx context.Context) context.Context {
	ctx, cancelFunc := context.WithCancel(ctx)
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigC
		fmt.Println("Received shut down request")
		cancelFunc()
	}()
	return ctx
}

type multipleError struct {
	errs []error
}

func NewMultipleError(errs ...error) error {
	var cleanedErrs []error
	for _, err := range errs {
		if err == nil {
			continue
		}
		cleanedErrs = append(cleanedErrs, err)
	}
	if len(cleanedErrs) == 0 {
		return nil
	}
	return multipleError{errs: cleanedErrs}
}

func (err multipleError) Error() string {
	var b strings.Builder
	b.WriteString("multiple errors encountered:")
	for _, e := range err.errs {
		b.WriteString("\n - ")
		b.WriteString(e.Error())
	}
	return b.String()
}

type Ticker struct {
	C chan struct{}
}

func NewTicker(period time.Duration, variation time.Duration) Ticker {
	t := Ticker{
		C: make(chan struct{}),
	}
	go func() {
		internalT := time.NewTicker(period)
		for {
			<-internalT.C
			// TODO:this doesn't make any sense, negative duration???
			time.Sleep(time.Duration(
				(rand.Float64()*2 - 1) * float64(variation.Nanoseconds()),
			))
			t.C <- struct{}{}
		}
	}()
	return t
}

func NewPerHourTicker(numTicksPerHour int, startOffset time.Duration) Ticker {
	// We arbitrarily do not support ticking more than once every 5 minutes
	if numTicksPerHour > 12 {
		numTicksPerHour = 12
	}
	if numTicksPerHour < 1 {
		numTicksPerHour = 1
	}
	if startOffset < 0 || startOffset >= time.Hour {
		startOffset = 0
	}
	t := Ticker{
		C: make(chan struct{}),
	}
	go func() {
		// TODO: make this less fragile and have it start earlier if numTicksPerHour > 0
		now := time.Now().UTC()
		startTime := now.Truncate(time.Hour).Add(time.Hour)
		time.Sleep(startTime.Sub(now))
		time.Sleep(startOffset)
		for hourT := time.Tick(time.Hour); ; <-hourT {
			for i := 0; i < numTicksPerHour; i++ {
				time.Sleep(time.Duration(
					rand.Float64() * float64(5*time.Minute),
				))
				t.C <- struct{}{}
				time.Sleep(time.Duration(int64(time.Hour) / int64(numTicksPerHour)))
			}
			t.C <- struct{}{}
		}
	}()
	return t
}

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
	return NewMultipleError(eg.errs...)
}
