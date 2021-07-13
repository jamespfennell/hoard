// Package hoard contains the public API of Hoard
package hoard

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/actions/audit"
	"github.com/jamespfennell/hoard/internal/actions/download"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/actions/pack"
	"github.com/jamespfennell/hoard/internal/actions/retrieve"
	"github.com/jamespfennell/hoard/internal/actions/upload"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/server"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util"
	"os"
	"path"
	"sync"
	"time"
)

const ManifestFileName = archive.ManifestFileName
const DownloadsSubDir = actions.DownloadsSubDir
const ArchivesSubDir = actions.ArchivesSubDir
const TmpSubDir = actions.TmpSubDir

// RunCollector runs a Hoard collection server.
func RunCollector(ctx context.Context, c *config.Config) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	var w sync.WaitGroup
	w.Add(1)
	var serverErr error
	go func() {
		serverErr = server.Run(ctx, c)
		// In the case of a server error, we want to cancel so that the periodic
		// tasks shut down and the binary exits in error. We cancel in all cases
		// to avoid resource leaks.
		cancelFunc()
		w.Done()
	}()
	for _, feed := range c.Feeds {
		feed := feed
		session := actions.NewSession(&feed, c.ObjectStorage, ctx, c.WorkspacePath, true)
		w.Add(4)
		go func() {
			download.RunPeriodically(session)
			w.Done()
		}()
		go func() {
			pack.RunPeriodically(session, c.PacksPerHour)
			w.Done()
		}()
		go func() {
			upload.RunPeriodically(session, c.UploadsPerHour)
			w.Done()
		}()
		go func() {
			audit.RunPeriodically(session)
			w.Done()
		}()
	}
	w.Wait()
	if serverErr != nil {
		serverErr = fmt.Errorf(
			"failed to start the Hoard server on port %d: %w", c.Port, serverErr)
		fmt.Println(serverErr)
	}
	return serverErr
}

func Download(c *config.Config) error {
	return executeInSession(c, download.RunOnce)
}

func Pack(c *config.Config) error {
	return executeInSession(c, func(session *actions.Session) error {
		return pack.RunOnce(session, false)
	})
}

func Merge(c *config.Config) error {
	return executeInSession(c, func(session *actions.Session) error {
		_, err := merge.RunOnce(session, session.LocalAStore())
		return err
	})
}

func Upload(c *config.Config) error {
	return executeInSession(c, upload.RunOnce)
}

func Audit(c *config.Config, startOpt *time.Time, end time.Time, fixProblems bool) error {
	return executeInSession(c, func(session *actions.Session) error {
		return audit.RunOnce(session, timeToHour(startOpt), *timeToHour(&end), fixProblems)
	})
}

type RetrieveOptions struct {
	Path            string
	KeepPacked      bool
	FlattenTimeDirs bool
	FlattenFeedDirs bool
	Start           time.Time
	End             time.Time
}

func Retrieve(c *config.Config, options RetrieveOptions) error {
	statusWriter := retrieve.NewStatusWriter(c.Feeds)
	return executeInSession(c, func(session *actions.Session) error {
		start := *timeToHour(&options.Start)
		end := *timeToHour(&options.End)
		if options.KeepPacked {
			return retrieve.RunOnceWithoutUnpacking(session, statusWriter, start, end,
				aStoreForRetrieval(session.Feed(), options.Path, options.FlattenFeedDirs, options.FlattenTimeDirs))
		}
		return retrieve.RunOnceWithUnpacking(session, statusWriter, start, end,
			dStoreForRetrieval(session.Feed(), options.Path, options.FlattenFeedDirs, options.FlattenTimeDirs))
	})
}

func Vacate(c *config.Config, removeWorkspace bool) error {
	err := util.NewMultipleError(Pack(c), Upload(c))
	if err != nil || !removeWorkspace {
		return err
	}
	err = os.RemoveAll(c.WorkspacePath)
	if err != nil {
		return fmt.Errorf("failed to remove workspace: %w", err)
	}
	return nil
}

func executeInSession(c *config.Config, f func(session *actions.Session) error) error {
	var eg util.ErrorGroup
	for _, feed := range c.Feeds {
		feed := feed
		session := actions.NewSession(&feed, c.ObjectStorage, context.Background(), c.WorkspacePath, false)
		eg.Add(1)
		f := func() {
			err := f(session)
			if err != nil {
				fmt.Printf("%s: failure: %s\n", feed.ID, err)
			}
			eg.Done(err)
		}
		if c.Sync {
			f()
		} else {
			go f()
		}
	}
	return eg.Wait()
}

// dStoreForRetrieval returns a DStore that the retrieve action can use to retrieve
// files to the target directories.
func dStoreForRetrieval(feed *config.Feed,
	root string, flattenFeeds bool, flattenTime bool) storage.WritableDStore {
	if !flattenFeeds {
		root = path.Join(root, feed.ID)
	}
	store := persistence.NewDiskPersistedStorage(root)
	if flattenTime {
		return dstore.NewFlatPersistedDStore(store)
	}
	return dstore.NewPersistedDStore(store)
}

// aStoreForRetrieval returns a AStore that the retrieve action can use to retrieve
// files to the target directories.
func aStoreForRetrieval(feed *config.Feed,
	root string, flattenFeeds bool, flattenTime bool) storage.WritableAStore {
	if !flattenFeeds {
		root = path.Join(root, feed.ID)
	}
	store := persistence.NewDiskPersistedStorage(root)
	if flattenTime {
		return astore.NewFlatPersistedAStore(store)
	}
	return astore.NewPersistedAStore(store)
}

func timeToHour(t *time.Time) *hour.Hour {
	if t == nil {
		return nil
	}
	hr := hour.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
	)
	return &hr
}
