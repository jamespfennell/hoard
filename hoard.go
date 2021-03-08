// Package hoard contains the public API of Hoard
package hoard

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions/audit"
	"github.com/jamespfennell/hoard/internal/actions/download"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/actions/pack"
	"github.com/jamespfennell/hoard/internal/actions/retrieve"
	"github.com/jamespfennell/hoard/internal/actions/upload"
	"github.com/jamespfennell/hoard/internal/server"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
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
const DownloadsSubDir = "downloads"
const ArchivesSubDir = "archives"

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
		sf := storeFactory{c: c, f: &feed, enableMonitoring: true, ctx: ctx}
		localDStore := sf.LocalDStore()
		localAStore := sf.LocalAStore()
		w.Add(2)
		go func() {
			download.PeriodicDownloader(ctx, &feed, localDStore)
			w.Done()
		}()
		go func() {
			pack.PeriodicPacker(ctx, &feed, c.PacksPerHour, localDStore, localAStore)
			w.Done()
		}()
		remoteAStore, err := sf.RemoteAStore()
		if err != nil {
			if _, ok := err.(NoRemoteStorageError); ok {
				fmt.Print("No remote storage configured! All files will be saved locally.\n")
				continue
			}
			return err
		}
		w.Add(1)
		go func() {
			upload.PeriodicUploader(ctx, &feed, c.UploadsPerHour, localAStore, remoteAStore)
			w.Done()
		}()
		separateAStores, err := sf.SeparateRemoteAStores()
		if err != nil {
			return err
		}
		w.Add(1)
		go func() {
			audit.PeriodicAuditor(ctx, &feed, separateAStores)
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
	return execute(c, func(feed *config.Feed, sf storeFactory) error {
		return download.Once(feed, sf.LocalDStore())
	})
}

func Pack(c *config.Config) error {
	return execute(c, func(feed *config.Feed, sf storeFactory) error {
		return pack.Pack(feed, sf.LocalDStore(), sf.LocalAStore(), false)
	})
}

func Merge(c *config.Config) error {
	return execute(c, func(feed *config.Feed, sf storeFactory) error {
		_, err := merge.Once(feed, sf.LocalAStore())
		return err
	})
}

func Upload(c *config.Config) error {
	return execute(c, func(feed *config.Feed, sf storeFactory) error {
		remoteAStore, err := sf.RemoteAStore()
		if err != nil {
			return err
		}
		return upload.Once(feed, sf.LocalAStore(), remoteAStore)
	})
}

func Audit(c *config.Config, startOpt *time.Time, end time.Time, fixProblems bool) error {
	return execute(c, func(feed *config.Feed, sf storeFactory) error {
		remoteAStores, err := sf.SeparateRemoteAStores()
		if err != nil {
			return err
		}
		return audit.Once(feed, fixProblems, remoteAStores,
			timeToHour(startOpt), *timeToHour(&end))
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
	return execute(c, func(feed *config.Feed, sf storeFactory) error {
		remoteAStore, err := sf.RemoteAStore()
		if err != nil {
			return err
		}
		start := *timeToHour(&options.Start)
		end := *timeToHour(&options.End)
		if options.KeepPacked {
			return retrieve.WithoutUnpacking(
				feed,
				remoteAStore,
				sf.AStoreForRetrieval(
					options.Path,
					options.FlattenFeedDirs,
					options.FlattenTimeDirs,
				),
				statusWriter,
				start,
				end,
			)
		}
		return retrieve.Regular(
			feed,
			remoteAStore,
			sf.DStoreForRetrieval(
				options.Path,
				options.FlattenFeedDirs,
				options.FlattenTimeDirs,
			),
			statusWriter,
			start,
			end,
		)
	})
}

func Vacate(c *config.Config, removeWorkspace bool) error {
	err := util.NewMultipleError(Pack(c), Upload(c))
	if err != nil || removeWorkspace {
		return err
	}
	err = os.RemoveAll(c.WorkspacePath)
	if err != nil {
		return fmt.Errorf("failed to remove workspace: %w", err)
	}
	return nil
}

func execute(c *config.Config, f func(feed *config.Feed, sf storeFactory) error) error {
	var eg util.ErrorGroup
	for _, feed := range c.Feeds {
		feed := feed
		eg.Add(1)
		f := func() {
			err := f(&feed, storeFactory{c: c, f: &feed, ctx: context.Background()})
			if err != nil {
				fmt.Printf("%s: failure: %s\n", feed.ID, err)
			}
			eg.Done(err)
		}
		if c.DisableConcurrency {
			f()
		} else {
			go f()
		}
	}
	return eg.Wait()
}

type storeFactory struct {
	c                *config.Config
	f                *config.Feed
	enableMonitoring bool
	ctx              context.Context
}

func (sf storeFactory) LocalDStore() storage.DStore {
	s := persistence.NewOnDiskByteStorage(path.Join(sf.c.WorkspacePath, DownloadsSubDir, sf.f.ID))
	if sf.enableMonitoring {
		go s.PeriodicallyReportUsageMetrics(sf.ctx, DownloadsSubDir, sf.f.ID)
	}
	return dstore.NewByteStorageBackedDStore(s)
}

func (sf storeFactory) DStoreForRetrieval(root string, flattenFeeds bool, flattenTime bool) storage.WritableDStore {
	if !flattenFeeds {
		root = path.Join(root, sf.f.ID)
	}
	s := persistence.NewOnDiskByteStorage(root)
	if flattenTime {
		return dstore.NewFlatByteStorageDStore(s)
	}
	return dstore.NewByteStorageBackedDStore(s)
}

func (sf storeFactory) LocalAStore() storage.AStore {
	s := persistence.NewOnDiskByteStorage(path.Join(sf.c.WorkspacePath, ArchivesSubDir, sf.f.ID))
	if sf.enableMonitoring {
		go s.PeriodicallyReportUsageMetrics(sf.ctx, ArchivesSubDir, sf.f.ID)
	}
	return astore.NewByteStorageBackedAStore(s)
}

func (sf storeFactory) AStoreForRetrieval(root string, flattenFeeds bool, flattenTime bool) storage.WritableAStore {
	if !flattenFeeds {
		root = path.Join(root, sf.f.ID)
	}
	s := persistence.NewOnDiskByteStorage(root)
	if flattenTime {
		return astore.NewFlatByteStorageAStore(s)
	}
	return astore.NewByteStorageBackedAStore(s)
}

type NoRemoteStorageError struct{}

func (err NoRemoteStorageError) Error() string {
	return "no remote storage configured"
}

func (sf storeFactory) SeparateRemoteAStores() ([]storage.AStore, error) {
	if len(sf.c.ObjectStorage) == 0 {
		return nil, NoRemoteStorageError{}
	}
	var remoteAStores []storage.AStore
	for _, objectStorage := range sf.c.ObjectStorage {
		objectStorage := objectStorage
		a, err := persistence.NewRemoteObjectStorage(
			sf.ctx,
			&objectStorage,
			sf.f,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initalize remote storage: %w", err)
		}
		if sf.enableMonitoring {
			go a.PeriodicallyReportUsageMetrics(sf.ctx)
		}
		remoteAStores = append(remoteAStores, astore.NewByteStorageBackedAStore(a))
	}
	return remoteAStores, nil
}

func (sf storeFactory) RemoteAStore() (storage.AStore, error) {
	stores, err := sf.SeparateRemoteAStores()
	return astore.NewMultiAStore(stores...), err
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
