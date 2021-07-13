// Package upload contains the upload action.
//
// This action uploads compressed archive files from local disk to remote object storage.
package upload

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/util"
	"time"
)

// RunPeriodically runs the upload action periodically with the prescribed period.
func RunPeriodically(session *actions.Session, uploadsPerHour int) {
	if session.RemoteAStore() == nil {
		fmt.Println("No remote object storage is configured, periodic uploader stopping")
		return
	}
	feed := session.Feed()
	fmt.Printf("Starting periodic uploader for %s\n", feed.ID)
	ticker := util.NewPerHourTicker(uploadsPerHour, time.Minute*12)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fmt.Printf("Uploading data for feed %s\n", feed.ID)
			err := RunOnce(session)
			fmt.Printf("Finished uploading data for feed %s (error=%s)\n", feed.ID, err)
			monitoring.RecordUpload(feed, err)
		case <-session.Ctx().Done():
			fmt.Printf("Stopped periodic uploader for %s\n", feed.ID)
			return
		}
	}
}

// RunONce runs the upload action once.
func RunOnce(session *actions.Session) error {
	if session.RemoteAStore() == nil {
		return fmt.Errorf("cannot upload because no remote object storage is configured")
	}
	aFiles, err := merge.RunOnce(session, session.LocalAStore())
	if err != nil {
		fmt.Printf(
			"Encountered error while merging local files: %s\n"+
				"Will continue with upload regardless\n", err)
	}
	var errs []error
	for _, aFile := range aFiles {
		err := uploadAFile(session, aFile)
		if err != nil {
			err = fmt.Errorf("Upload failed for %s, %w\n", aFile, err)
			fmt.Println(err)
			errs = append(errs, err)
		}
	}
	return util.NewMultipleError(errs...)
}

func uploadAFile(session *actions.Session, aFile storage.AFile) error {
	fmt.Printf("%s: beginning upload\n", aFile)
	if err := storage.CopyAFile(session.LocalAStore(), session.RemoteAStore(), aFile); err != nil {
		return err
	}
	fmt.Printf("%s: finished upload\n", aFile)
	fmt.Printf("%s: merging remote archives\n", aFile)
	// The delete failing should not stop the merge from being attempted and vice-versa
	// so we run each operation irrespective of the result of the other.
	return util.NewMultipleError(
		session.LocalAStore().Delete(aFile),
		merge.RunOnceForHour(session, session.RemoteAStore(), aFile.Hour),
	)
}
