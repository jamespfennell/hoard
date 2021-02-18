package server

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"net/http"
	"sync"
	"time"
)

//go:embed index.html
var indexHtml string

var startTime = time.Now().UTC()

func Run(c *config.Config, interruptChan <-chan struct{}) error {
	// TODO: if there is an error here it should crash the program
	// TODO: graceful shutdown
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// TODO: pretty print the duration
		_, err := io.WriteString(w,
			fmt.Sprintf(indexHtml,
				time.Now().UTC().Sub(startTime).Truncate(time.Second),
				c))
		if err != nil {
			fmt.Println("error handling http request", err)
		}
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", c.Port)}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-interruptChan
		_ = srv.Shutdown(context.Background())
		wg.Done()
	}()
	err := srv.ListenAndServe()
	fmt.Println("Waiting for server to shutdown")
	wg.Wait()
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}
