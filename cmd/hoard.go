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

	c2 := config.NewDefaultConfig()
	err = yaml.Unmarshal(b, &c2)
	if err != nil {
		fmt.Println("Could not read config file", err)
		os.Exit(1)
	}
	hoard.RunServer(c2, interruptC)
}
