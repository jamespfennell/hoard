package merge

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
	"runtime"
)

// Merging is CPU intensive so we rate limit the number of concurrent operations
// TODO: reconsider this
var pool = util.NewWorkerPool(runtime.NumCPU())

func Once(f *config.Feed, a storage.AStore) ([]storage.AFile, error) {
	searchResults, err := a.Search(nil, hour.Now())
	if err != nil {
		return nil, err
	}
	var aFiles []storage.AFile
	var errs []error
	for _, searchResult := range searchResults {
		searchResult := searchResult
		pool.Run(context.Background(), func() {
			fmt.Printf("Merging hour %s for feed %s\n", searchResult.Hour, f.ID)
			aFile, err := mergeHour(f, a, searchResult.Hour)
			fmt.Printf("Finished merging hour %s for feed %s (err=%s)\n", searchResult.Hour, f.ID, err)
			if err == nil {
				aFiles = append(aFiles, aFile)
			}
			errs = append(errs, err)
		})
	}
	return aFiles, util.NewMultipleError(errs...)
}

func DoHour(f *config.Feed, astore storage.AStore, hour hour.Hour) error {
	var err error
	pool.Run(context.Background(), func() {
		_, err = mergeHour(f, astore, hour)
	})
	if err != nil {
		fmt.Printf("Error merging hour: %s\n", err)
	}
	return err
}

func mergeHour(f *config.Feed, aStore storage.AStore, hour hour.Hour) (storage.AFile, error) {
	aFiles, err := storage.ListAFilesInHour(aStore, hour)
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
	for _, aFile := range aFiles {
		fmt.Printf("- %s\n", aFile)
	}
	// TODO: need to write to disk
	a, err := archive.CreateFromAFiles(f, aFiles, aStore, dstore.NewInMemoryDStore())
	if err != nil {
		return storage.AFile{}, err
	}
	if err := aStore.Store(a.AFile(), a.Reader()); err != nil {
		_ = a.Close()
		return storage.AFile{}, err
	}
	if err := a.Close(); err != nil {
		return storage.AFile{}, err
	}
	fmt.Printf("Uploaded the archive; deleting old archives\n")
	for _, aFile := range a.IncorporatedAFiles {
		if aFile == a.AFile() {
			continue
		}
		if err := aStore.Delete(aFile); err != nil {
			fmt.Printf("Error deleting file after merging: %s\n", err)
		}
	}
	return a.AFile(), nil
}
