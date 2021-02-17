// Package hoard contains the public API of Hoard
package hoard

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/collector"
	"github.com/jamespfennell/hoard/internal/download"
	"github.com/jamespfennell/hoard/internal/merge"
	"github.com/jamespfennell/hoard/internal/pack"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/upload"
	"github.com/jamespfennell/hoard/internal/workerpool"
	"path"
	"sync"
)

const ManifestFileName = archive.ManifestFileName
const DownloadsSubDir = "downloads"
const ArchivesSubDir = "archives"

// RunCollector runs a Hoard collection server.
func RunCollector(c *config.Config, interruptChan <-chan struct{}) error {
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		feed := feed
		sf := storeFactory{c: c, f: &feed}
		localDStore := sf.LocalDStore()
		localAStore := sf.LocalAStore()
		remoteAStore := sf.RemoteAStore()
		w.Add(3)
		go func() {
			download.PeriodicDownloader(&feed, localDStore, interruptChan)
			w.Done()
		}()
		go func() {
			pack.PeriodicPacker(&feed, localDStore, localAStore, interruptChan)
			w.Done()
		}()
		go func() {
			upload.PeriodicUploader(&feed, localAStore, remoteAStore, interruptChan)
			w.Done()
		}()
	}
	// TODO: graceful shutdown
	// w.Add(1)
	go func() {
		// TODO: think about the error here
		collector.Run(c, interruptChan)
		// w.Done()
	}()
	w.Wait()
	fmt.Println("Stopping Hoard server")
	return nil
}

func Vacate() {}

func Download(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		return download.Once(feed, sf.LocalDStore())
	})
}

func Pack(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, sf storeFactory) error {
		return pack.Pack(feed, sf.LocalDStore(), sf.LocalAStore())
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

func executeConcurrently(c *config.Config, f func(feed *config.Feed, sf storeFactory) error) error {
	var eg workerpool.ErrorGroup
	for _, feed := range c.Feeds {
		feed := feed
		eg.Add(1)
		go func() {
			err := f(&feed, storeFactory{c: c, f: &feed})
			if err != nil {
				fmt.Printf("%s: failure: %s\n", feed.ID, err)
			} else {
				fmt.Printf("%s: success\n", feed.ID)
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
	return dstore.NewByteStorageBackedDStore(
		persistence.NewOnDiskByteStorage(path.Join(sf.c.WorkspacePath, DownloadsSubDir, sf.f.ID)),
	)
}

func (sf storeFactory) LocalAStore() storage.AStore {
	return astore.NewByteStorageBackedAStore(
		persistence.NewOnDiskByteStorage(path.Join(sf.c.WorkspacePath, ArchivesSubDir, sf.f.ID)),
	)
}

func (sf storeFactory) RemoteAStore() storage.AStore {
	if len(sf.c.ObjectStorage) > 0 {
		a, err := persistence.NewS3ObjectStorage(sf.c.ObjectStorage[0],
			path.Join(sf.c.ObjectStorage[0].Prefix, sf.f.ID),
		)
		if err != nil {
			// TODO: handle the error
		}
		return astore.NewByteStorageBackedAStore(a)
	}
	return nil
}
