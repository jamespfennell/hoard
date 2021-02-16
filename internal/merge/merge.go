package merge

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/workerpool"
	"runtime"
	"time"
)

// Merging is CPU intensive so we rate limit the number of concurrent operations
var pool = workerpool.NewWorkerPool(runtime.NumCPU())

func Once(f *config.Feed, a storage.AStore) error {
	hours, err := a.ListNonEmptyHours()
	if err != nil {
		return err
	}
	var g workerpool.ErrorGroup
	for _, hour := range hours {
		hour := hour
		g.Add(1)
		pool.Run(func() {
			fmt.Printf("Merging hour %s for feed %s\n", time.Time(hour), f.ID)
			g.Done(mergeHour(f, a, hour))
		})
	}
	return g.Wait()
}

func mergeHour(f *config.Feed, astore storage.AStore, hour storage.Hour) error {
	aFiles, err := astore.ListInHour(hour)
	if err != nil {
		return err
	}
	if len(aFiles) <= 1 {
		return nil
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
				return err
			}
			sourceArchive, err := archive.NewArchiveFromSerialization(b)
			if err != nil {
				// TODO: don't error out fully, continue to process other AFiles
				// TODO: delete the file, it's corrupted
				return err
			}
			copyResult, err := storage.Copy(sourceArchive, ar, hour)
			if err != nil {
				// This error is unrecoverable: we may have corrupted the archive
				// while copying
				return err
			}
			if len(copyResult.CopyErrors) > 0 {
				// TODO Unwrap error what's that about
				return fmt.Errorf("failed to copy all files")
			}
			if err := ar.AddSourceManifest(sourceArchive); err != nil {
				// TODO: don't error out fully, continue to process other AFiles
				return err
			}
		}
		l = ar.Lock()
	}
	// TODO: delete the old A Files
	content, err := l.Serialize()
	if err != nil {
		return err
	}
	newAFile := storage.AFile{
		Prefix: f.Prefix(),
		Time:   hour,
		Hash:   l.Hash(),
	}
	if err := astore.Store(newAFile, content); err != nil {
		return err
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
	return nil
}
