package upload

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/merge"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/util"
	"time"
)

func PeriodicUploader(ctx context.Context, feed *config.Feed, localAStore storage.AStore, remoteAStore storage.AStore) {
	fmt.Printf("Starting periodic uploader for %s\n", feed.ID)
	// TODO: honor the configuration value for this
	timer := util.NewPerHourTicker(1, time.Minute*12)
	for {
		select {
		case <-timer.C:
			err := Once(feed, localAStore, remoteAStore)
			monitoring.RecordUpload(feed, err)
		case <-ctx.Done():
			fmt.Printf("Stopped periodic uploader for %s\n", feed.ID)
			return
		}
	}
}

// TODO skipCurrentHour param
func Once(f *config.Feed, localAStore storage.AStore, remoteAStore storage.AStore) error {
	aFiles, err := merge.Once(f, localAStore)
	if err != nil {
		fmt.Printf(
			"Encountered error while merging local files: %s\n"+
				"Will continue with upload regardless\n", err)
	}
	var errs []error
	for _, aFile := range aFiles {
		err := uploadAFile(f, aFile, localAStore, remoteAStore)
		if err != nil {
			err = fmt.Errorf("Upload failed for %s, %w\n", aFile, err)
			fmt.Println(err)
			errs = append(errs, err)
		}
	}
	return util.NewMultipleError(errs...)
}

func uploadAFile(f *config.Feed, aFile storage.AFile, localAStore storage.AStore, remoteAStore storage.AStore) error {
	fmt.Printf("%s: beginning upload\n", aFile)
	content, err := localAStore.Get(aFile)
	if err != nil {
		return err
	}
	if err := remoteAStore.Store(aFile, content); err != nil {
		return err
	}
	fmt.Printf("%s: finished upload\n", aFile)
	fmt.Printf("%s: merging remote archives\n", aFile)
	// The delete failing should not stop the merge from being attempted and vice-versa
	// so we run each operation irrespective of the result of the other.
	return util.NewMultipleError(
		localAStore.Delete(aFile),
		merge.DoHour(f, remoteAStore, aFile.Time),
	)
}
