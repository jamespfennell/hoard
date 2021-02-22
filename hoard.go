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
	"github.com/jamespfennell/hoard/internal/actions/upload"
	"github.com/jamespfennell/hoard/internal/server"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util"
	"path"
	"sync"
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
		sf := storeFactory{c: c, f: &feed}
		localDStore := sf.LocalDStore()
		localAStore := sf.LocalAStore()
		remoteAStore := sf.RemoteAStore()
		w.Add(3)
		go func() {
			download.PeriodicDownloader(ctx, &feed, localDStore)
			w.Done()
		}()
		go func() {
			pack.PeriodicPacker(ctx, &feed, localDStore, localAStore)
			w.Done()
		}()
		go func() {
			upload.PeriodicUploader(ctx, &feed, localAStore, remoteAStore)
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

func Vacate() {}

func Download(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		return download.Once(feed, sf.LocalDStore())
	})
}

func Pack(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		return pack.Pack(feed, sf.LocalDStore(), sf.LocalAStore(), false)
	})
}

func Merge(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		_, err := merge.Once(feed, sf.LocalAStore())
		return err
	})
}

func Upload(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		return upload.Once(feed, sf.LocalAStore(), sf.RemoteAStore())
	})
}

func Audit(c *config.Config, fixProblems bool) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		return audit.Once(feed, fixProblems, sf.RemoteAStores())
	})
}

func executeConcurrently(c *config.Config, f func(feed *config.Feed, sf storeFactory) error) error {
	var eg util.ErrorGroup
	for _, feed := range c.Feeds {
		feed := feed
		eg.Add(1)
		go func() {
			err := f(&feed, storeFactory{c: c, f: &feed})
			if err != nil {
				fmt.Printf("%s: failure: %s\n", feed.ID, err)
			} else {
				// fmt.Printf("%s: success\n", feed.ID)
			}
			eg.Done(err)
		}()
	}
	return eg.Wait()
}

type storeFactory struct {
	c *config.Config
	f *config.Feed
}

func (sf storeFactory) LocalDStore() storage.DStore {
	s := persistence.NewOnDiskByteStorage(path.Join(sf.c.WorkspacePath, DownloadsSubDir, sf.f.ID))
	go s.PeriodicallyReportUsageMetrics(DownloadsSubDir, sf.f.ID)
	return dstore.NewByteStorageBackedDStore(s)
}

func (sf storeFactory) LocalAStore() storage.AStore {
	s := persistence.NewOnDiskByteStorage(path.Join(sf.c.WorkspacePath, ArchivesSubDir, sf.f.ID))
	go s.PeriodicallyReportUsageMetrics(ArchivesSubDir, sf.f.ID)
	return astore.NewByteStorageBackedAStore(s)
}

func (sf storeFactory) RemoteAStores() []storage.AStore {
	// TODO: handle 0 AStores
	var remoteAStores []storage.AStore
	for _, objectStorage := range sf.c.ObjectStorage {
		objectStorage := objectStorage
		a, err := persistence.NewS3ObjectStorage(
			&objectStorage,
			sf.f,
		)
		if err != nil {
			// TODO: handle the error
		}
		remoteAStores = append(remoteAStores, astore.NewByteStorageBackedAStore(a))
	}
	return remoteAStores
}

func (sf storeFactory) RemoteAStore() storage.AStore {
	return astore.NewMultiAStore(sf.RemoteAStores()...)
}
