package upload

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/util"
	"time"
)

func PeriodicUploader(ctx context.Context, feed *config.Feed, uploadsPerHour int, localAStore storage.AStore, remoteAStore storage.AStore) {
	fmt.Printf("Starting periodic uploader for %s\n", feed.ID)
	ticker := util.NewPerHourTicker(uploadsPerHour, time.Minute*12)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fmt.Printf("Uploading data for feed %s\n", feed.ID)
			err := Once(feed, localAStore, remoteAStore)
			fmt.Printf("Finished uploading data for feed %s (error=%s)\n", feed.ID, err)
			monitoring.RecordUpload(feed, err)
		case <-ctx.Done():
			fmt.Printf("Stopped periodic uploader for %s\n", feed.ID)
			return
		}
	}
}

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
	if err := storage.CopyAFile(localAStore, remoteAStore, aFile); err != nil {
		return err
	}
	fmt.Printf("%s: finished upload\n", aFile)
	fmt.Printf("%s: merging remote archives\n", aFile)
	// The delete failing should not stop the merge from being attempted and vice-versa
	// so we run each operation irrespective of the result of the other.
	return util.NewMultipleError(
		localAStore.Delete(aFile),
		merge.DoHour(f, remoteAStore, aFile.Hour),
	)
}
