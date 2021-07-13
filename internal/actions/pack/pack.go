// Package pack contains the pack action.
//
// This action searches for raw downloaded files in local disk, and collects them
// into compressed archive files.
package pack

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
	"time"
)

// RunPeriodically runs the pack action periodically, with the period specified
// in the second input argument.
func RunPeriodically(session *actions.Session, packsPerHour int) {
	feed := session.Feed()
	fmt.Printf("Starting periodic packer for %s\n", feed.ID)
	ticker := util.NewPerHourTicker(packsPerHour, time.Minute*2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			currentTime := time.Now().UTC()
			skipCurrentHour := currentTime.Sub(currentTime.Truncate(time.Hour)) < 10*time.Minute
			err := RunOnce(session, skipCurrentHour)
			if err != nil {
				fmt.Printf("Encountered error in periodic packing: %s", err)
			}
			monitoring.RecordPack(feed, err)
		case <-session.Ctx().Done():
			fmt.Printf("Stopped periodic packer for %s\n", feed.ID)
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
			fmt.Println("Skipping packing for current hour")
			continue
		}
		fmt.Printf("%s: packing hour %s\n", session.Feed().ID, hr)
		errs = append(errs, packHour(session.Feed(), dStore, session.LocalAStore(), hr))
	}
	return util.NewMultipleError(errs...)
}

func packHour(f *config.Feed, dStore storage.DStore, aStore storage.AStore, hour hour.Hour) error {
	dFiles, err := dStore.ListInHour(hour)
	if err != nil {
		return err
	}
	_, incorporatedDFiles, err := archive.CreateFromDFiles(f, dFiles, dStore, aStore)
	if err != nil {
		return err
	}
	fmt.Printf("%s: deleting %d files\n", f.ID, len(incorporatedDFiles))
	for _, dFile := range incorporatedDFiles {
		if err := dStore.Delete(dFile); err != nil {
			monitoring.RecordPackFileErrors(f, err)
			fmt.Print(err)
		}
	}
	return nil
}
