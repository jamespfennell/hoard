// Package hoard contains the public API of Hoard
package hoard

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/server"
	"time"
)

// RunServer runs a Hoard collection server.
func RunServer(c config.Config, workspacePath string, port int, interruptChan <-chan struct{}) {
	server.Run(c, workspacePath, port, interruptChan)
}

func Clean() {}

func Retrieve(c config.Config, feedIds []string, startTime time.Time, endTime time.Time, outputPath string,
	mergeFeeds, mergeTimes bool) error {
	return nil
}

