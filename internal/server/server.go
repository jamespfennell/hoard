package server

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/download"
	"github.com/jamespfennell/hoard/internal/pack"
	a "github.com/jamespfennell/hoard/internal/storage/astore"
	d "github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"path"
	"sync"
)

func Run(c config.Config, interruptChan <-chan struct{}) {
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		astore := a.NewByteStorageBackedAStore(
			persistence.NewOnDiskByteStorage(path.Join(c.WorkspacePath, "archives", feed.ID)),
		)
		downloads := persistence.NewOnDiskByteStorage(path.Join(c.WorkspacePath, "downloads", feed.ID))
		dstore := d.NewByteStorageBackedDStore(downloads)

		feed := feed
		w.Add(2)
		go func() {
			download.PeriodicDownloader(&feed, dstore, interruptChan)
			w.Done()
		}()
		go func() {
			pack.PeriodicPacker(&feed, dstore, astore, interruptChan)
			w.Done()
		}()
	}

	go func() {
		// TODO: if there is an error here it should crash the program
		// TODO: graceful shutdown
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", c.Port), nil))
	}()
	w.Wait()
	log.Print("Stopping Hoard server")
}
