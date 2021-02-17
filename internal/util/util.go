package util

import (
	"math/rand"
	"strings"
	"time"
)

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
		// TODO: there is limitation here that until the first hour
		//  there are no ticks even if numTickerPerHour is large
		now := time.Now().UTC()
		startTime := now.Truncate(time.Hour).Add(time.Hour)
		time.Sleep(startTime.Sub(now))
		time.Sleep(startOffset)
		for hourT := time.Tick(time.Hour); ; <-hourT {
			for i := 0; i < numTicksPerHour; i++ {
				// TODO: fuzz the ticks over a five minute period
				//  so that occurances to not occur together
				t.C <- struct{}{}
				time.Sleep(time.Duration(int64(time.Hour) / int64(numTicksPerHour)))
			}
			t.C <- struct{}{}
		}
	}()
	return t
}