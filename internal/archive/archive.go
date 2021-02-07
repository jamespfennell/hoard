package archive

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"log"
	"time"
)

func PeriodicArchiver(feed *config.Feed, dstore dstore.DStore, astore astore.AStore, interruptChan <-chan struct{}) {
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

func Archive(f *config.Feed, d dstore.DStore, a astore.AStore) error {
	hours, err := d.ListNonEmptyHours()
	if err != nil {
		return err
	}
	for _, hour := range hours {
		// TODO: archive the hours in parallel and use an error group
		archiveHour(f, d, a, hour)
	}
	return nil
}

func archiveHour(f *config.Feed, d dstore.DStore, a astore.AStore, hour storage.Hour) error {

	fmt.Println("Archiving ", hour)
	dfiles, err := d.ListInHour(hour)
	if err != nil {
		return err
	}
	fmt.Println("Found", len(dfiles), " d files!")
	for _, d := range dfiles {
		fmt.Println(d)
	}
	return nil
}
