package server

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/download"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"log"
	"path"
	"sync"
)

func Run(c config.Config, workspaceRoot string, port int, interruptChan <-chan struct{}) {
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		astore := persistence.NewByteStorageBackedDStore(
			persistence.NewOnDiskByteStorage(path.Join(workspaceRoot, "archives", feed.ID)),
		)
		dstore := persistence.NewByteStorageBackedDStore(
			persistence.NewOnDiskByteStorage(path.Join(workspaceRoot, "downloads", feed.ID)),
		)
		feed := feed
		w.Add(2)
		go func() {
			download.PeriodicDownloader(&feed, dstore, interruptChan)
			w.Done()
		}()
		go func() {
			archive.PeriodicArchiver(&feed, dstore, astore, interruptChan)
			w.Done()
		}()
	}
	w.Wait()
	log.Print("Stopping Hoard server")
}
