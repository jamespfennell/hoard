package pack

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"log"
	"time"
)

func PeriodicPacker(feed *config.Feed, dstore storage.DStore, astore storage.AStore, interruptChan <-chan struct{}) {
	log.Print("starting packer", feed)
	// TODO: start at a given time
	timer := time.NewTicker(time.Minute * 16) //feed.Periodicity)
	for {
		select {
		case <-timer.C:
			Pack(feed, dstore, astore)
		case <-interruptChan:
			log.Print("Stopped feed archiving for", feed.ID)
			return
		}
	}
}

func Pack(f *config.Feed, d storage.DStore, a storage.AStore) error {
	hours, err := d.ListNonEmptyHours()
	if err != nil {
		// TODO: log
		fmt.Println("Failed?", err)
		return err
	}
	for _, hour := range hours {
		// TODO: pack the hours in parallel and use an error group?
		//  Probably should rate limit this
		//  Maybe use a global worker pool
		fmt.Println("Packing", f)

		err := packHour(f, d, a, hour)
		if err != nil {
			monitoring.RecordPackFileErrors(f, err)
			// TODO: log this
		}
		monitoring.RecordPack(f, err)
	}
	// TODO: if there are errors, propagate them up the call stack?
	return nil
}

func packHour(f *config.Feed, d storage.DStore, a storage.AStore, hour storage.Hour) error {
	var l *archive.LockedArchive
	var copyResult storage.CopyResult
	// We enclose the Archive variable in a scope to ensure it doesn't accidentally
	// get used after being locked
	{
		ar := archive.NewArchiveForWriting(hour)
		var err error
		copyResult, err = storage.Copy(d, ar, hour)
		if err != nil {
			return err
		}
		if len(copyResult.CopyErrors) > 0 {
			monitoring.RecordPackFileErrors(f, copyResult.CopyErrors...)
			// TODO: log this
		}
		if len(copyResult.DFilesCopied) == 0 {
			// TODO: return an error
			return nil
		}
		l = ar.Lock()
	}
	content, err := l.Serialize()
	if err != nil {
		return err
	}

	aFile := storage.AFile{
		Prefix: f.Prefix(),
		Time:   hour,
		Hash:   l.Hash(),
	}
	if err := a.Store(aFile, content); err != nil {
		return err
	}
	fmt.Println("F deleting files", copyResult.DFilesCopied)
	for _, dFile := range copyResult.DFilesCopied {
		if err := d.Delete(dFile); err != nil {
			monitoring.RecordPackFileErrors(f, err)
			// TODO: log the error
		}
	}
	monitoring.RecordPackSizes(f, copyResult.BytesCopied, len(content))
	return nil
}
