// Package download contains the functions used to download files to disk
package download

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/util"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func PeriodicDownloader(feed *config.Feed, dstore storage.DStore, interruptChan <-chan struct{}) {
	log.Print("starting downloader", feed)
	timer := util.NewTicker(feed.Periodicity, feed.Variation)
	client := &http.Client{}
	var lastHash storage.Hash
	for {
		select {
		case <-timer.C:
			dFile, err := downloadOnce(feed, dstore, lastHash, client, defaultTimeGetter)
			monitoring.RecordDownload(feed, err)
			if err != nil {
				// TODO Log this properly
				fmt.Println("Error", err)
				continue
			}
			lastHash = dFile.Hash
		case <-interruptChan:
			log.Printf("Stopped packing for feed %q\n", feed.ID)
			return
		}
	}
}

// Once runs a single download cycle for the feed
func Once(feed *config.Feed, d storage.DStore) error {
	client := &http.Client{}
	_, err := downloadOnce(feed, d, "", client, defaultTimeGetter)
	return err
}

type timeGetter func() time.Time

func defaultTimeGetter() time.Time {
	return time.Now().UTC()
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func downloadOnce(feed *config.Feed, dstore storage.DStore, lastHash storage.Hash, client httpClient, now timeGetter) (*storage.DFile, error) {
	req, err := http.NewRequest("GET", feed.URL, nil)
	if err != nil {
		return nil, err
	}
	for key, value := range feed.Headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status recieved: %d / %s", resp.StatusCode, resp.Status)
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	hash := storage.CalculateHash(bytes)
	dFile := storage.DFile{
		Prefix:  feed.Prefix(),
		Postfix: feed.Postfix,
		Time:    now(),
		Hash:    hash,
	}
	if hash == lastHash {
		return &dFile, nil
	}
	err = dstore.Store(dFile, bytes)
	if err != nil {
		return nil, err
	}
	monitoring.RecordSavedDownload(feed, len(bytes))
	return &dFile, nil
}
