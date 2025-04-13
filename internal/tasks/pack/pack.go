// Package pack contains the pack task.
//
// This task searches for raw downloaded files in local disk, and collects them
// into compressed archive files.
package pack

import (
	"fmt"
	"time"

	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/util"
)

type pack struct {
	packsPerHour     int
	alwaysPackRecent bool
}

func New(packsPerHour int, alwaysPackRecent bool) tasks.Task {
	return &pack{packsPerHour: packsPerHour, alwaysPackRecent: alwaysPackRecent}
}

func (p *pack) PeriodicTicker(session *tasks.Session) *util.Ticker {
	t := util.NewPerHourTicker(p.packsPerHour, time.Minute*2)
	return &t
}

func (p *pack) Run(session *tasks.Session) error {
	currentTime := time.Now().UTC()
	currentHourIsRecent := currentTime.Sub(currentTime.Truncate(time.Hour)) < 10*time.Minute
	return runOnce(session, !p.alwaysPackRecent && currentHourIsRecent)
}

func (p *pack) Name() string {
	return "pack"
}

// runOnce runs the pack task once.
//
// If skipCurrentHour is true, any DFiles created in the current hour will be ignored.
func runOnce(session *tasks.Session, skipCurrentHour bool) error {
	dStore := session.LocalDStore()
	hours, err := dStore.ListNonEmptyHours()
	if err != nil {
		return err
	}
	currentHour := hour.Now()
	var errs []error
	for _, hr := range hours {
		if skipCurrentHour && hr == currentHour {
			session.LogWithHour(hr).Debug("Skipping packing the current hour")
			continue
		}
		session.LogWithHour(hr).Debug("Packing hour")
		errs = append(errs, packHour(session, hr))
	}
	return util.NewMultipleError(errs...)
}

func packHour(session *tasks.Session, hour hour.Hour) error {
	dStore := session.LocalDStore()
	dFiles, err := dStore.ListInHour(hour)
	if err != nil {
		return err
	}
	_, incorporatedDFiles, err := archive.CreateFromDFiles(session.Feed(), dFiles, dStore, session.LocalAStore())
	if err != nil {
		return err
	}
	session.LogWithHour(hour).Debug(fmt.Sprintf("Deleting %d downloaded files", len(incorporatedDFiles)))
	for _, dFile := range incorporatedDFiles {
		if err := dStore.Delete(dFile); err != nil {
			monitoring.RecordPackFileErrors(session.Feed(), err)
			session.LogWithHour(hour).Error(fmt.Sprintf("Failed to delete DFile %s: %s", dFile, err))
		}
	}
	return nil
}
