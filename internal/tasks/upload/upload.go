// Package upload contains the upload task.
//
// This task uploads compressed archive files from local disk to remote object storage.
package upload

import (
	"fmt"
	"time"

	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/tasks/merge"
	"github.com/jamespfennell/hoard/internal/util"
)

// RunPeriodically runs the upload task periodically with the prescribed period.
func RunPeriodically(session *tasks.Session, uploadsPerHour int, skipMerging bool) {
	if session.RemoteAStore() == nil {
		session.Log().Warn("No remote object storage is configured, periodic uploader will not run")
		return
	}
	feed := session.Feed()
	session.Log().Info("Starting periodic uploader")
	ticker := util.NewPerHourTicker(uploadsPerHour, time.Minute*12)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			session.Log().Debug("Beginning data upload")
			err := RunOnce(session, skipMerging)
			if err != nil {
				session.Log().Error(fmt.Sprintf("Error during data upload: %s", err))
			} else {
				session.Log().Debug("Finished data upload")
			}
			monitoring.RecordUpload(feed, err)
		case <-session.Ctx().Done():
			session.Log().Info("Stopped periodic uploader")
			return
		}
	}
}

// RunOnce runs the upload task once.
func RunOnce(session *tasks.Session, skipMerging bool) error {
	if session.RemoteAStore() == nil {
		session.Log().Error("Cannot upload because no remote object storage is configured")
		return fmt.Errorf("cannot upload because no remote object storage is configured")
	}
	aFiles, err := merge.RunOnce(session, session.LocalAStore())
	if err != nil {
		session.Log().Error(fmt.Sprintf("Encountered error while merging local files: %s\n"+
			"Will continue with upload anyway", err))
	}
	var errs []error
	for _, aFile := range aFiles {
		err := uploadAFile(session, aFile, skipMerging)
		if err != nil {
			err = fmt.Errorf("upload error for %s: %w", aFile, err)
			session.Log().Error(err.Error())
			errs = append(errs, err)
		}
	}
	return util.NewMultipleError(errs...)
}

func uploadAFile(session *tasks.Session, aFile storage.AFile, skipMerging bool) error {
	session.Log().Debug(fmt.Sprintf("Beginning upload of %s", aFile))
	if err := storage.CopyAFile(session.LocalAStore(), session.RemoteAStore(), aFile); err != nil {
		session.Log().Error(fmt.Sprintf("Error while uploading %s: %s", aFile, err))
		return err
	}
	session.Log().Debug(fmt.Sprintf("Finished upload of %s", aFile))
	session.Log().Debug("Merging remote archives")
	// The delete operation failing should not stop the merge from being attempted and vice-versa.
	deleteErr := session.LocalAStore().Delete(aFile)
	var mergeErr error
	if !skipMerging {
		mergeErr = merge.RunOnceForHour(session, session.RemoteAStore(), aFile.Hour)
	}
	return util.NewMultipleError(deleteErr, mergeErr)
}
