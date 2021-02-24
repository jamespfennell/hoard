package merge

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/util"
	"runtime"
	"sync"
	"time"
)

// Merging is CPU intensive so we rate limit the number of concurrent operations
var pool = util.NewWorkerPool(runtime.NumCPU())

func Once(f *config.Feed, a storage.AStore) ([]storage.AFile, error) {
	searchResults, err := a.ListNonEmptyHours()
	if err != nil {
		return nil, err
	}
	var aFiles []storage.AFile
	var m sync.Mutex
	var g util.ErrorGroup
	for _, searchResult := range searchResults {
		searchResult := searchResult
		g.Add(1)
		pool.Run(func() {
			fmt.Printf("Merging hour %s for feed %s\n", time.Time(searchResult.Hour()), f.ID)
			aFile, err := mergeHour(f, a, searchResult.Hour())
			if err == nil {
				m.Lock()
				defer m.Unlock()
				aFiles = append(aFiles, aFile)
			}
			g.Done(err)
		})
	}
	return aFiles, g.Wait()
}

func DoHour(f *config.Feed, astore storage.AStore, hour storage.Hour) error {
	_, err := mergeHour(f, astore, hour)
	if err != nil {
		fmt.Printf("Error merging hour: %s\n", err)
	}
	return err
}

func mergeHour(f *config.Feed, astore storage.AStore, hour storage.Hour) (storage.AFile, error) {
	aFiles, err := astore.ListInHour(hour)
	if err != nil {
		return storage.AFile{}, err
	}
	if len(aFiles) == 0 {
		return storage.AFile{}, fmt.Errorf("unexpected empty hour %v in AStore", hour)
	}
	if len(aFiles) == 1 {
		return aFiles[0], nil
	}
	var l *archive.LockedArchive
	// We enclose the Archive variable in a scope to ensure it doesn't accidentally
	// get used after being locked
	{
		ar := archive.NewArchiveForWriting(hour)
		for _, aFile := range aFiles {
			b, err := astore.Get(aFile)
			if err != nil {
				fmt.Printf("unable to retrieve AFile %s for merging: %s\n", aFile, err)
				continue
			}
			sourceArchive, err := archive.NewArchiveFromSerialization(b)
			if err != nil {
				fmt.Printf("unable to deserialize AFile %s for merging: %s\n", aFile, err)
				continue
			}
			copyResult, err := storage.Copy(sourceArchive, ar, hour)
			if err != nil {
				// This error is unrecoverable: we may have corrupted the archive
				// while copying
				return storage.AFile{}, fmt.Errorf(
					"unrecoverable error while copying files into archive: %w", err)
			}
			if len(copyResult.CopyErrors) > 0 {
				return storage.AFile{}, fmt.Errorf(
					"unrecoverable error while copying files into archive: %w",
					util.NewMultipleError(copyResult.CopyErrors...))
			}
			if err := ar.AddSourceManifest(sourceArchive); err != nil {
				fmt.Printf("failed to write manifest %s; continuing regardless\n", err)
			}
		}
		l = ar.Lock()
	}
	content, err := l.Serialize()
	if err != nil {
		return storage.AFile{}, err
	}
	newAFile := storage.AFile{
		Prefix: f.Prefix(),
		Time:   hour,
		Hash:   l.Hash(),
	}
	if err := astore.Store(newAFile, content); err != nil {
		return storage.AFile{}, err
	}
	for _, aFile := range aFiles {
		if aFile == newAFile {
			continue
		}
		if err := astore.Delete(aFile); err != nil {
			fmt.Printf("Error deleting file after merging: %s\n", err)
		}
	}
	return newAFile, nil
}