package main

import (
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	interruptC := make(chan struct{})
	go func() {
		<-sigC
		close(interruptC)
	}()
	c := config.Config{
		Feeds: []config.Feed{
			{
				ID:          "PATH1",
				Prefix:      "PATH1_",
				Postfix:     ".gtfsrt",
				URL:         "https://path.transitdata.nyc/gtfsrt",
				Periodicity: 500 * time.Millisecond,
			},
			{
				ID:          "PATH2",
				Prefix:      "PATH2_",
				Postfix:     ".gtfsrt",
				URL:         "https://path.transitdata.nyc/gtfsrt",
				Periodicity: 600 * time.Millisecond,
			},
		},
	}
	hoard.RunServer(c, "tmp", 10000, interruptC)

}
