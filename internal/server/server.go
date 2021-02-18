package server

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

//go:embed index.html
var indexHtml string

var startTime = time.Now().UTC()

// Populated by the compiler; seconds since the Unix epoc
var buildTimeUnix string

func buildTime() time.Time {
	i, err := strconv.Atoi(buildTimeUnix)
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return time.Unix(int64(i), 0).UTC()
}

func Run(ctx context.Context, c *config.Config) error {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := io.WriteString(w,
			fmt.Sprintf(
				indexHtml,
				time.Now().UTC().Sub(startTime).Truncate(time.Second),
				c,
				buildTime(),
			))
		if err != nil {
			fmt.Println("error handling http request", err)
		}
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", c.Port)}
	ctx, cancelFunc := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
		wg.Done()
	}()
	fmt.Println("Starting HTTP server on port", c.Port)
	err := srv.ListenAndServe()
	fmt.Println("Waiting for HTTP server to stop")
	cancelFunc()
	wg.Wait()
	fmt.Println("HTTP server stopped")
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}
