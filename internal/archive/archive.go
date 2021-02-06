package archive

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"log"
	"time"
)

func PeriodicArchiver(feed *config.Feed, dstore storage.DStore, astore storage.AStore, interruptChan <-chan struct{}) {
	log.Print("starting archiver", feed)
	// TODO: start at a given time
	timer := time.NewTicker(feed.Periodicity)
	for {
		select {
		case <-timer.C:
			Archive(feed, dstore, astore)
		case <-interruptChan:
			log.Print("Stopped feed archiving for", feed.ID)
			return
		}
	}
}

func Archive(feed *config.Feed, dstore storage.DStore, astore storage.AStore) {
	// List all hours
}
