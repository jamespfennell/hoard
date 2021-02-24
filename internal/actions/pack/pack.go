package pack

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/util"
	"time"
)

func PeriodicPacker(ctx context.Context, feed *config.Feed, dstore storage.DStore, astore storage.AStore) {
	fmt.Printf("Starting periodic packer for %s\n", feed.ID)
	// TODO: honor the configuration value for this and also in skipCurrentHour
	timer := util.NewPerHourTicker(1, time.Minute*2)
	for {
		select {
		case <-timer.C:
			if err := Pack(feed, dstore, astore, true); err != nil {
				fmt.Printf("Encountered error in periodic packing: %s", err)
			}
		case <-ctx.Done():
			fmt.Printf("Stopped periodic packer for %s\n", feed.ID)
			return
		}
	}
}

func Pack(f *config.Feed, d storage.DStore, a storage.AStore, skipCurrentHour bool) error {
	hours, err := d.ListNonEmptyHours()
	if err != nil {
		return err
	}
	currentHour := time.Now().UTC().Truncate(time.Hour)
	var errs []error
	for _, hour := range hours {
		if skipCurrentHour && time.Time(hour) == currentHour {
			fmt.Println("Skipping packing for current hour")
			continue
		}
		fmt.Printf("%s: packing hour %s\n", f.ID, hour)
		err := packHour(f, d, a, hour)
		if err != nil {
			monitoring.RecordPackFileErrors(f, err)
			errs = append(errs, err)
		}
		monitoring.RecordPack(f, err)
	}
	return util.NewMultipleError(errs...)
}

func packHour(f *config.Feed, d storage.DStore, a storage.AStore, hour storage.Hour) error {
	var l *archive.LockedArchive
	var copyResult storage.CopyResult
	// We enclose the Archive variable in a scope to ensure it doesn't accidentally
	// get used after being locked
	{
		ar := archive.NewArchiveForWriting(hour)
		var err error
		copyResult, err = storage.Copy(d, ar, hour)
		if err != nil {
			return err
		}
		if len(copyResult.CopyErrors) > 0 {
			monitoring.RecordPackFileErrors(f, copyResult.CopyErrors...)
			fmt.Printf("Errors copying files for packing: %s", copyResult.CopyErrors)
		}
		if len(copyResult.DFilesCopied) == 0 {
			// TODO: return an error
			return nil
		}
		l = ar.Lock()
	}
	content, err := l.Serialize()
	if err != nil {
		return err
	}

	aFile := storage.AFile{
		Prefix: f.Prefix(),
		Time:   hour,
		Hash:   l.Hash(),
	}
	if err := a.Store(aFile, content); err != nil {
		return err
	}
	fmt.Printf("%s: deleting %d files\n", f.ID, len(copyResult.DFilesCopied))
	for _, dFile := range copyResult.DFilesCopied {
		if err := d.Delete(dFile); err != nil {
			monitoring.RecordPackFileErrors(f, err)
			fmt.Print(err)
		}
	}
	monitoring.RecordPackSizes(f, copyResult.BytesCopied, len(content))
	return nil
}