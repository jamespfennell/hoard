package server

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed index.html
var indexHtml string

var startTime = time.Now().UTC()

// Populated by the compiler; seconds since the Unix epoc
var buildTimeUnix string

// Populated by the compiler; the git commit hash the binary was built from
var gitCommit string

func buildDay() string {
	i, err := strconv.Atoi(buildTimeUnix)
	if err != nil {
		return "unknown"
	}
	return time.Unix(int64(i), 0).UTC().Format("2006-01-02")
}

func version() string {
	if gitCommit == "" {
		return "unknown"
	}
	short := gitCommit
	if len(short) > 7 {
		short = short[:7]
	}
	return fmt.Sprintf(`<a href="https://github.com/jamespfennell/hoard/commit/%s">%s</a>`, gitCommit, short)
}

type durationUnit struct {
	name string
	d    time.Duration
}

var durationUnits = []durationUnit{
	{"year", 365 * 24 * time.Hour},
	{"day", 24 * time.Hour},
	{"hour", time.Hour},
	{"minute", time.Minute},
	{"second", time.Second},
}

func pluralize(n int64, unit string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", unit)
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

// approximateDuration returns the duration in the largest unit that fits, e.g. "3 hours".
func approximateDuration(d time.Duration) string {
	for _, unit := range durationUnits {
		if d >= unit.d {
			return pluralize(int64(d/unit.d), unit.name)
		}
	}
	return pluralize(0, "second")
}

// preciseDuration returns the duration in days, hours and minutes, e.g. "2 days, 3 hours and 14 minutes".
// Durations under a minute are given in seconds.
func preciseDuration(d time.Duration) string {
	if d < time.Minute {
		return pluralize(int64(d/time.Second), "second")
	}
	var parts []string
	for _, unit := range durationUnits[1:4] {
		n := int64(d / unit.d)
		d -= time.Duration(n) * unit.d
		if n > 0 {
			parts = append(parts, pluralize(n, unit.name))
		}
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}

func Run(ctx context.Context, c *config.Config, log *slog.Logger) error {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		uptime := time.Now().UTC().Sub(startTime)
		_, err := io.WriteString(w,
			fmt.Sprintf(
				indexHtml,
				preciseDuration(uptime),
				approximateDuration(uptime),
				c,
				version(),
				buildDay(),
			))
		if err != nil {
			log.Error(fmt.Sprintf("error handling http request: %s", err))
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
	log.Info(fmt.Sprintf("starting HTTP server on port %d", c.Port))
	err := srv.ListenAndServe()
	log.Info("waiting for HTTP server to stop")
	cancelFunc()
	wg.Wait()
	log.Info("HTTP server stopped")
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}
