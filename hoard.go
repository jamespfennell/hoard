package hoard

import (
	"fmt"
	"time"
)

type Config struct {
	Feeds []struct {
		ID string
	}
	ObjectStorage []struct {
		ID string
	}
}

func NewConfigFromURL(url string) (Config, error) {
	return Config{}, nil
}

type Session struct{}

func NewSession() (*Session, error) {
	return &Session{}, nil
}

func (s *Session) Collect(interruptC <-chan struct{}) {
	timer := time.NewTicker(time.Second)
	for {
		select {
		case <-timer.C:
			fmt.Println("Time")
		case <-interruptC:
			fmt.Println("Closing...")
			return
		}
	}
}
