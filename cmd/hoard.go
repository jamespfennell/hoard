package main

import (
	"flag"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

var configLocation = flag.String("config_file", "hoard.yml", "help")

// TODO: port flag

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

	flag.Parse()
	b, err := ioutil.ReadFile(*configLocation)
	if err != nil {
		fmt.Println("Could not read config file", err)

		os.Exit(1)
	}
	fmt.Println(string(b))

	var c2 config.Config
	err = yaml.Unmarshal(b, &c2)
	if err != nil {
		fmt.Println("Could not read config file", err)
		os.Exit(1)
	}
	/*
		c2 := config.Config{
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

	*/
	hoard.RunServer(c2, "tmp", 10000, interruptC)
}
