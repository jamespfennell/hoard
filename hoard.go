// Package hoard contains the public API of Hoard
package hoard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"sync"
	"time"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/server"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/tasks/audit"
	"github.com/jamespfennell/hoard/internal/tasks/download"
	"github.com/jamespfennell/hoard/internal/tasks/merge"
	"github.com/jamespfennell/hoard/internal/tasks/pack"
	"github.com/jamespfennell/hoard/internal/tasks/retrieve"
	"github.com/jamespfennell/hoard/internal/tasks/upload"
	"github.com/jamespfennell/hoard/internal/util"
)

const ManifestFileName = archive.ManifestFileName
const DownloadsSubDir = tasks.DownloadsSubDir
const ArchivesSubDir = tasks.ArchivesSubDir
const TmpSubDir = tasks.TmpSubDir

// RunCollector runs a Hoard collection server.
func RunCollector(ctx context.Context, c *config.Config) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	log := newLogger(c)
	var w sync.WaitGroup
	w.Add(1)
	var serverErr error
	go func() {
		serverErr = server.Run(ctx, c, log)
		// In the case of a server error, we want to cancel so that the periodic
		// tasks shut down and the binary exits in error. We cancel in all cases
		// to avoid resource leaks.
		cancelFunc()
		w.Done()
	}()
	for _, feed := range c.Feeds {
		feed := feed
		session := tasks.NewSession(&feed, c, log, ctx, true)
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
			audit.RunPeriodically(session, !c.DisableMerging)
			w.Done()
		}()
	}
	w.Wait()
	if serverErr != nil {
		serverErr = fmt.Errorf(
			"failed to start the Hoard server on port %d: %w", c.Port, serverErr)
		slog.Error(serverErr.Error())
	}
	return serverErr
}

func Download(c *config.Config) error {
	return executeInSession(c, download.RunOnce)
}

func Pack(c *config.Config) error {
	return executeInSession(c, func(session *tasks.Session) error {
		return pack.RunOnce(session, false)
	})
}

func Merge(c *config.Config) error {
	return executeInSession(c, func(session *tasks.Session) error {
		_, err := merge.RunOnce(session, session.LocalAStore())
		return err
	})
}

func Upload(c *config.Config) error {
	return executeInSession(c, upload.RunOnce)
}

func Audit(c *config.Config, startOpt *time.Time, end time.Time, enforceCompression bool, fixProblems bool) error {
	return executeInSession(c, func(session *tasks.Session) error {
		return audit.RunOnce(session, timeToHour(startOpt), *timeToHour(&end),
			!c.DisableMerging, enforceCompression, fixProblems)
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
	return executeInSession(c, func(session *tasks.Session) error {
		start := *timeToHour(&options.Start)
		end := *timeToHour(&options.End)
		if options.KeepPacked {
			return retrieve.RunOnceWithoutUnpacking(session, statusWriter, start, end,
				aStoreForRetrieval(session.Feed(), options, session.Log()))
		}
		return retrieve.RunOnceWithUnpacking(session, statusWriter, start, end,
			dStoreForRetrieval(session.Feed(), options, session.Log()))
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

func newLogger(c *config.Config) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevelParsed()}))
}

func executeInSession(c *config.Config, f func(session *tasks.Session) error) error {
	var eg util.ErrorGroup
	log := newLogger(c)
	for _, feed := range c.Feeds {
		feed := feed
		session := tasks.NewSession(&feed, c, log, context.Background(), false)
		eg.Add(1)
		f := func() {
			err := f(session)
			if err != nil {
				session.Log().Error(fmt.Sprintf("failure: %s", err))
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

// dStoreForRetrieval returns a DStore that the retrieve task can use to retrieve
// files to the target directories.
func dStoreForRetrieval(feed *config.Feed, options RetrieveOptions, log *slog.Logger) storage.WritableDStore {
	root := options.Path
	if !options.FlattenFeedDirs {
		root = path.Join(root, feed.ID)
	}
	store := persistence.NewDiskPersistedStorage(root)
	if options.FlattenTimeDirs {
		return dstore.NewFlatPersistedDStore(store)
	}
	return dstore.NewPersistedDStore(store, log)
}

// aStoreForRetrieval returns a AStore that the retrieve task can use to retrieve
// files to the target directories.
func aStoreForRetrieval(feed *config.Feed, options RetrieveOptions, log *slog.Logger) storage.WritableAStore {
	root := options.Path
	if !options.FlattenFeedDirs {
		root = path.Join(root, feed.ID)
	}
	store := persistence.NewDiskPersistedStorage(root)
	if options.FlattenTimeDirs {
		return astore.NewFlatPersistedAStore(store)
	}
	return astore.NewPersistedAStore(store, log)
}

func timeToHour(t *time.Time) *hour.Hour {
	if t == nil {
		return nil
	}
	hr := hour.FromTime(*t)
	return &hr
}
