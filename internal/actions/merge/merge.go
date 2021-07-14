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
		fmt.Printf("Merging hour %s for feed %s\n", searchResult.Hour, session.Feed().ID)
		aFile, err := mergeHour(session, aStore, searchResult.Hour)
		fmt.Printf("Finished merging hour %s for feed %s (err=%s)\n", searchResult.Hour, session.Feed().ID, err)
		if err == nil {
			aFiles = append(aFiles, aFile)
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
		fmt.Printf("Error merging hour: %s\n", err)
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
	fmt.Printf("Going to merge %d AFiles:\n", len(aFiles))
	dStore, eraseDStore := session.TempDStore()
	defer eraseDStore()
	aStore, eraseAStore := session.TempAStore()
	defer eraseAStore()
	for _, aFile := range aFiles {
		fmt.Printf("- %s\n", aFile)
		if err := storage.CopyAFile(sourceAStore, aStore, aFile); err != nil {
			return storage.AFile{}, err
		}
	}

	var newAFile storage.AFile
	var incorporatedAFiles []storage.AFile
	pool.Run(context.Background(), func() {
		newAFile, incorporatedAFiles, err = archive.CreateFromAFiles(session.Feed(), aFiles, aStore, aStore, dStore)
	})
	if err != nil {
		return storage.AFile{}, err
	}
	if err := storage.CopyAFile(aStore, sourceAStore, newAFile); err != nil {
		return storage.AFile{}, err
	}
	fmt.Printf("Uploaded the archive; deleting old archives\n")
	for _, aFile := range incorporatedAFiles {
		if aFile.Equals(newAFile) {
			continue
		}
		fmt.Printf("Deleting from remote storage: %s", aFile)
		if err := sourceAStore.Delete(aFile); err != nil {
			fmt.Printf("Error deleting file after merging: %s\n", err)
		}
	}
	return newAFile, nil
}
