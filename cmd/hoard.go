package main

import (
	"github.com/jamespfennell/hoard"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	s, _ := hoard.NewSession()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	interruptC := make(chan struct{})
	go func() {
		<-sigC
		interruptC <- struct{}{}
	}()
	s.Collect(interruptC)
}
