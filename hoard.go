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
	"path"
	"sync"
)

const ManifestFileName = archive.ManifestFileName
const DownloadsSubDir = "downloads"
const ArchivesSubDir = "archives"

// RunCollector runs a Hoard collection server.
func RunCollector(c *config.Config, interruptChan <-chan struct{}) error {
	collector.Run(c, interruptChan)
	return nil
}

func Vacate() {}

func Download(c *config.Config) error {
	ctx := newContext(c)
	// TODO: have an error group merger
	// TODO: extract all this runner code to its own function
	var mainErr error
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		feed := feed
		w.Add(1)
		go func() {
			err := download.Once(&feed, ctx.feedIDToFeedContext[feed.ID].localDStore)
			if err != nil {
				fmt.Printf("%s: failure: %s\n", feed.ID, err)
				mainErr = err
			} else {
				fmt.Printf("%s: success\n", feed.ID)
			}
			w.Done()
		}()
	}
	w.Wait()
	return mainErr
}

func Pack(c *config.Config) error {
	ctx := newContext(c)
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		feed := feed
		w.Add(1)
		go func() {
			pack.Pack(&feed, ctx.feedIDToFeedContext[feed.ID].localDStore,
				ctx.feedIDToFeedContext[feed.ID].localAStore)
			w.Done()
		}()
	}
	w.Wait()
	return nil
}

func Merge(c *config.Config) error {
	return executeConcurrently(c, func(feed *config.Feed, ctx feedContext) error {
		return merge.Once(feed, ctx.localAStore)
	})
}

func executeConcurrently(c *config.Config, f func(feed *config.Feed, ctx feedContext) error) error {
	ctx := newContext(c)
	// TODO: have a concurrency-safe error group merger
	var mainErr error
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		feed := feed
		w.Add(1)
		go func() {
			err := f(&feed, ctx.feedIDToFeedContext[feed.ID])
			if err != nil {
				fmt.Printf("%s: failure: %s\n", feed.ID, err)
				mainErr = err
			} else {
				fmt.Printf("%s: success\n", feed.ID)
			}
			w.Done()
		}()
	}
	w.Wait()
	return mainErr
}

type feedContext struct {
	dFileStorage persistence.ByteStorage
	aFileStorage persistence.ByteStorage
	localDStore  storage.DStore
	localAStore  storage.AStore
}

type context struct {
	feedIDToFeedContext map[string]feedContext
}

func newContext(c *config.Config) context {
	ctx := context{feedIDToFeedContext: map[string]feedContext{}}
	for _, feed := range c.Feeds {
		feedCtx := feedContext{}
		feedCtx.dFileStorage = persistence.NewOnDiskByteStorage(path.Join(c.WorkspacePath, DownloadsSubDir, feed.ID))
		feedCtx.aFileStorage = persistence.NewOnDiskByteStorage(path.Join(c.WorkspacePath, ArchivesSubDir, feed.ID))
		feedCtx.localDStore = dstore.NewByteStorageBackedDStore(feedCtx.dFileStorage)
		feedCtx.localAStore = astore.NewByteStorageBackedAStore(feedCtx.aFileStorage)
		ctx.feedIDToFeedContext[feed.ID] = feedCtx
	}
	return ctx
}
