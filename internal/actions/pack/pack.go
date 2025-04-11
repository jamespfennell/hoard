// Package pack contains the pack action.
//
// This action searches for raw downloaded files in local disk, and collects them
// into compressed archive files.
package pack

import (
	"time"

	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
)

// RunPeriodically runs the pack action periodically, with the period specified
// in the second input argument.
func RunPeriodically(session *actions.Session, packsPerHour int) {
	feed := session.Feed()
	session.Log().Info("Starting periodic packer")
	ticker := util.NewPerHourTicker(packsPerHour, time.Minute*2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			currentTime := time.Now().UTC()
			skipCurrentHour := currentTime.Sub(currentTime.Truncate(time.Hour)) < 10*time.Minute
			err := RunOnce(session, skipCurrentHour)
			if err != nil {
				session.Log().Errorf("Error while packing: %s", err)
			}
			monitoring.RecordPack(feed, err)
		case <-session.Ctx().Done():
			session.Log().Info("Stopped periodic packer")
			return
		}
	}
}

// RunOnce runs the pack action once.
//
// If skipCurrentHour is true, any DFiles created in the current hour will be ignored.
func RunOnce(session *actions.Session, skipCurrentHour bool) error {
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

func packHour(session *actions.Session, hour hour.Hour) error {
	dStore := session.LocalDStore()
	dFiles, err := dStore.ListInHour(hour)
	if err != nil {
		return err
	}
	_, incorporatedDFiles, err := archive.CreateFromDFiles(session.Feed(), dFiles, dStore, session.LocalAStore())
	if err != nil {
		return err
	}
	session.LogWithHour(hour).Debugf("Deleting %d downloaded files", len(incorporatedDFiles))
	for _, dFile := range incorporatedDFiles {
		if err := dStore.Delete(dFile); err != nil {
			monitoring.RecordPackFileErrors(session.Feed(), err)
			session.LogWithHour(hour).Errorf("Failed to delete DFile %s: %s", dFile, err)
		}
	}
	return nil
}
