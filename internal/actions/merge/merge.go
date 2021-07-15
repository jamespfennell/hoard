// Package merge contains the merge action.
//
// This action searches for multiple archive files for the same hour and
// merges them together.
package merge

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
	"runtime"
	"strings"
)

// Merging is CPU intensive so we rate limit the number of concurrent operations
var pool = util.NewWorkerPool(runtime.NumCPU())

// RunOnce runs the merge operation once for the provided AStore.
func RunOnce(session *actions.Session, aStore storage.AStore) ([]storage.AFile, error) {
	searchResults, err := aStore.Search(nil, hour.Now())
	if err != nil {
		return nil, err
	}
	var aFiles []storage.AFile
	var errs []error
	for _, searchResult := range searchResults {
		searchResult := searchResult
		session.LogWithHour(searchResult.Hour).Debug("Merging hour")
		aFile, err := mergeHour(session, aStore, searchResult.Hour)
		if err == nil {
			session.LogWithHour(searchResult.Hour).Debug("Merged hour with no errors")
			aFiles = append(aFiles, aFile)
		} else {
			session.LogWithHour(searchResult.Hour).Errorf("Error while merging: %s", err)
		}
		errs = append(errs, err)
	}
	return aFiles, util.NewMultipleError(errs...)
}

// RunOnceForHour runs the merge operation on any AFiles in the provided AStore that
// correspond to the provided hour.
func RunOnceForHour(session *actions.Session, aStore storage.AStore, hour hour.Hour) error {
	_, err := mergeHour(session, aStore, hour)
	if err != nil {
		session.LogWithHour(hour).Errorf("Error merging hour: %s\n", err)
	}
	return err
}

func mergeHour(session *actions.Session, sourceAStore storage.AStore, hour hour.Hour) (storage.AFile, error) {
	aFiles, err := storage.ListAFilesInHour(sourceAStore, hour)
	if err != nil {
		return storage.AFile{}, err
	}
	if len(aFiles) == 0 {
		return storage.AFile{}, fmt.Errorf("unexpected empty hour %v in AStore", hour)
	}
	if len(aFiles) == 1 {
		return aFiles[0], nil
	}

	dStore, eraseDStore := session.TempDStore()
	defer func() {
		if err := eraseDStore(); err != nil {
			session.LogWithHour(hour).Errorf("Failed to erase temporary DStore: %s", err)
		}
	}()
	aStore, eraseAStore := session.TempAStore()
	defer func() {
		if err := eraseAStore(); err != nil {
			session.LogWithHour(hour).Errorf("Failed to erase temporary AStore: %s", err)
		}
	}()

	var logMessage strings.Builder
	_, _ = fmt.Fprintf(&logMessage, "Going to merge %d AFiles:\n", len(aFiles))
	for _, aFile := range aFiles {
		_, _ = fmt.Fprintf(&logMessage, "* %s\n", aFile)
		if err := storage.CopyAFile(sourceAStore, aStore, aFile); err != nil {
			return storage.AFile{}, err
		}
	}
	session.LogWithHour(hour).Debug(logMessage)

	var newAFile storage.AFile
	var incorporatedAFiles []storage.AFile
	pool.Run(context.Background(), func() {
		session.LogWithHour(hour).Debug("Merge operation started")
		newAFile, incorporatedAFiles, err = archive.CreateFromAFiles(session.Feed(), aFiles, aStore, aStore, dStore)
		session.LogWithHour(hour).Debug("Merge operation completed")
	})
	if err != nil {
		return storage.AFile{}, err
	}
	if err := storage.CopyAFile(aStore, sourceAStore, newAFile); err != nil {
		return storage.AFile{}, err
	}
	session.LogWithHour(hour).Debug("Uploaded the archive; proceeding to delete old archives")
	for _, aFile := range incorporatedAFiles {
		if aFile.Equals(newAFile) {
			continue
		}
		session.LogWithHour(hour).Debugf("Deleting from remote storage: %s", aFile)
		if err := sourceAStore.Delete(aFile); err != nil {
			session.LogWithHour(hour).Errorf("Failed to delete archive file %s after merging: %s", aFile, err)
		}
	}
	return newAFile, nil
}
