package pack

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
	"time"
)

func PeriodicPacker(ctx context.Context, feed *config.Feed, packsPerHour int, dstore storage.DStore, astore storage.AStore) {
	fmt.Printf("Starting periodic packer for %s\n", feed.ID)
	ticker := util.NewPerHourTicker(packsPerHour, time.Minute*2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			currentTime := time.Now().UTC()
			skipCurrentHour := currentTime.Sub(currentTime.Truncate(time.Hour)) < 10*time.Minute
			err := Pack(feed, dstore, astore, skipCurrentHour)
			if err != nil {
				fmt.Printf("Encountered error in periodic packing: %s", err)
			}
			monitoring.RecordPack(feed, err)
		case <-ctx.Done():
			fmt.Printf("Stopped periodic packer for %s\n", feed.ID)
			return
		}
	}
}

func Pack(f *config.Feed, dStore storage.DStore, aStore storage.AStore, skipCurrentHour bool) error {
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
		fmt.Printf("%s: packing hour %s\n", f.ID, hr)
		errs = append(errs, packHour(f, dStore, aStore, hr))
	}
	return util.NewMultipleError(errs...)
}

func packHour(f *config.Feed, dStore storage.DStore, aStore storage.AStore, hour hour.Hour) error {
	dFiles, err := dStore.ListInHour(hour)
	if err != nil {
		return err
	}
	arc, err := archive.CreateFromDFiles(f, dFiles, dStore)
	if err != nil {
		return err
	}
	if err := aStore.Store(arc.AFile(), arc.Reader()); err != nil {
		_ = arc.Close()
		return err
	}
	if err := arc.Close(); err != nil {
		return err
	}
	fmt.Printf("%s: deleting %d files\n", f.ID, len(arc.IncorporatedDFiles))
	for _, dFile := range arc.IncorporatedDFiles {
		if err := dStore.Delete(dFile); err != nil {
			monitoring.RecordPackFileErrors(f, err)
			fmt.Print(err)
		}
	}
	// TODO: how to handle this?
	monitoring.RecordPackSizes(f, 0, 0)
	return nil
}
