package merge

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/workerpool"
	"runtime"
	"sync"
	"time"
)

// Merging is CPU intensive so we rate limit the number of concurrent operations
var pool = workerpool.NewWorkerPool(runtime.NumCPU())

func Once(f *config.Feed, a storage.AStore) ([]storage.AFile, error) {
	hours, err := a.ListNonEmptyHours()
	if err != nil {
		return nil, err
	}
	var aFiles []storage.AFile
	var m sync.Mutex
	var g workerpool.ErrorGroup
	for _, hour := range hours {
		hour := hour
		g.Add(1)
		pool.Run(func() {
			fmt.Printf("Merging hour %s for feed %s\n", time.Time(hour), f.ID)
			aFile, err := mergeHour(f, a, hour)
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
	return err
}

func mergeHour(f *config.Feed, astore storage.AStore, hour storage.Hour) (storage.AFile, error) {
	aFiles, err := astore.ListInHour(hour)
	if err != nil {
		return storage.AFile{}, err
	}
	if len(aFiles) <= 1 {
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
				// TODO: don't error out fully, continue to process other AFiles
				return storage.AFile{}, err
			}
			sourceArchive, err := archive.NewArchiveFromSerialization(b)
			if err != nil {
				// TODO: don't error out fully, continue to process other AFiles
				// TODO: delete the file, it's corrupted
				return storage.AFile{}, err
			}
			copyResult, err := storage.Copy(sourceArchive, ar, hour)
			if err != nil {
				// This error is unrecoverable: we may have corrupted the archive
				// while copying
				return storage.AFile{}, err
			}
			if len(copyResult.CopyErrors) > 0 {
				// TODO Unwrap error what's that about
				return storage.AFile{}, fmt.Errorf("failed to copy all files")
			}
			if err := ar.AddSourceManifest(sourceArchive); err != nil {
				// TODO: don't error out fully, continue to process other AFiles
				return storage.AFile{}, err
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
			// TODO: it is critical that we test this case
			continue
		}
		if err := astore.Delete(aFile); err != nil {
			// TODO: log the error
		}
	}
	return newAFile, nil
}
