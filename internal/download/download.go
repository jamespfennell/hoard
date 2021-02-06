// Package download contains the functions used to download files to disk
package download

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func PeriodicDownloader(feed *config.Feed, dstore dstore.DStore, interruptChan <-chan struct{}) {
	log.Print("starting downloader", feed)
	timer := time.NewTicker(feed.Periodicity)
	var lastHash storage.Hash
	for {
		select {
		case <-timer.C:
			dFile, err := downloadFeed(feed, dstore, lastHash, get,
				func() time.Time {
					return time.Now().UTC()
				})
			if err != nil {
				fmt.Println("Error", err)
				continue
			}
			lastHash = dFile.Hash
		case <-interruptChan:
			log.Print("Stopped feed collection for", feed.ID)
			return
		}
	}
}

type httpGetter func(string) ([]byte, error)

func get(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return bytes, resp.Body.Close()
}

type timeGetter func() time.Time

func downloadFeed(feed *config.Feed, dstore dstore.DStore, lastHash storage.Hash, get httpGetter, now timeGetter) (*storage.DFile, error) {
	bytes, err := get(feed.URL)
	if err != nil {
		return nil, err
	}
	hash, err := storage.CalculateHash(bytes)
	if err != nil {
		return nil, err
	}
	dFile := storage.DFile{
		Prefix:  feed.Prefix,
		Postfix: feed.Postfix,
		Time:    now(),
		Hash:    hash,
	}
	if hash == lastHash {
		// TODO: don't skip if this is a new hour
		return &dFile, nil
	}
	return &dFile, dstore.Store(dFile, bytes)
}
