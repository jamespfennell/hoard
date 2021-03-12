package pack

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
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

func Pack(f *config.Feed, d storage.DStore, a storage.AStore, skipCurrentHour bool) error {
	hours, err := d.ListNonEmptyHours()
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
		errs = append(errs, packHour(f, d, a, hr))
	}
	return util.NewMultipleError(errs...)
}

func packHour(f *config.Feed, d storage.DStore, a storage.AStore, hour hour.Hour) error {
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
			// Note that copy errors can never be triggered by writing to
			// to the archive, so even if there are errors we continue with
			// the archive
			monitoring.RecordPackFileErrors(f, copyResult.CopyErrors...)
			fmt.Printf("Errors copying files for packing: %s", copyResult.CopyErrors)
		}
		if len(copyResult.DFilesCopied) == 0 {
			return fmt.Errorf("failed to copy any filed into the archive")
		}
		l = ar.Lock()
	}
	content, err := l.Serialize()
	if err != nil {
		return err
	}

	aFile := storage.AFile{
		Prefix: f.Prefix(),
		Hour:   hour,
		Hash:   l.Hash(),
	}
	if err := a.Store(aFile, bytes.NewReader(content)); err != nil {
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
