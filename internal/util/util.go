package util

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"io"
	"math/rand"
	"net/http"
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

var publicIPAddress struct {
	value *string
	mutex sync.RWMutex
}

// TODO: use sync.Once
func GetPublicIPAddress() (string, bool) {
	publicIPAddress.mutex.Lock()
	defer publicIPAddress.mutex.Unlock()
	if publicIPAddress.value != nil {
		return *publicIPAddress.value, true
	}
	sites := []string{
		"checkip.amazonaws.com",
		"ifconfig.me",
		"icanhazip.com",
		"ipecho.net/plain",
		"ifconfig.co",
	}
	var ipAddress *string
	for _, site := range sites {
		res, err := http.Get("http://" + site)
		if err != nil || res.StatusCode != http.StatusOK {
			continue
		}
		if res.ContentLength > 15 {
			continue
		}
		ipAddressRaw, err := io.ReadAll(res.Body)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(ipAddressRaw))
		ipAddress = &s
		fmt.Printf("Determined IP address %s using %s\n", *ipAddress, site)
		break
	}
	if ipAddress == nil {
		return "", false
	}
	publicIPAddress.value = ipAddress
	return *ipAddress, true
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
	if len(cleanedErrs) == 1 {
		return cleanedErrs[0]
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
	C    chan struct{}
	done chan struct{}
}

func (t Ticker) Stop() {
	close(t.done)
}

func NewTicker(period time.Duration, variation time.Duration) Ticker {
	t := Ticker{
		C:    make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		t.C <- struct{}{}
		internalT := time.NewTicker(period)
		defer internalT.Stop()
		for {
			select {
			case <-internalT.C:
				if wait(time.Duration(rand.Float64()*float64(variation)), t.done) {
					t.C <- struct{}{}
				}
			case <-t.done:
				return
			}
		}
	}()
	return t
}

// wait blocks until the provided duration has passed or until the done
// channel is closed, whichever is first. It returns true if and only if
// the duration has passed.
func wait(duration time.Duration, done <-chan struct{}) bool {
	if duration == 0 {
		return true
	}
	timer := time.NewTimer(duration)
	select {
	case <-timer.C:
		return true
	case <-done:
		if !timer.Stop() {
			// This is to handle the race condition in which the timer
			// fires after the done channel, but before the we call Stop.
			// If we didn't drain the channel, there would be a goroutine
			// leak.
			<-timer.C
		}
		return false
	}
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
		C:    make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		now := time.Now().UTC()
		startTime := now.Truncate(time.Hour).Add(time.Hour)
		wait(startTime.Sub(now), t.done) // wait until the next hour
		wait(startOffset, t.done)
		hourTicker := NewTicker(time.Hour, 0)
		defer hourTicker.Stop()
		for {
			select {
			case <-hourTicker.C:
				wait(time.Duration(rand.Float64()*float64(5*time.Minute)), t.done)
				for i := 0; i < numTicksPerHour-1; i++ {
					t.C <- struct{}{}
					wait(time.Duration(int64(time.Hour)/int64(numTicksPerHour)), t.done)
				}
				t.C <- struct{}{}
			case <-t.done:
				return
			}
		}
	}()
	return t
}

type WorkerPool struct {
	sem *semaphore.Weighted
}

func (pool *WorkerPool) Run(ctx context.Context, f func()) {
	if err := pool.sem.Acquire(ctx, 1); err != nil {
		fmt.Printf("Failed to acquire semaphore: %s\n", err)
	}
	defer pool.sem.Release(1)
	f()
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	return &WorkerPool{
		sem: semaphore.NewWeighted(int64(numWorkers)),
	}
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
