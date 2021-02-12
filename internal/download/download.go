// Package download contains the functions used to download files to disk
package download

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type Ticker struct {
	C chan struct{}
	t *time.Ticker
}

func NewTicker(period time.Duration, variation time.Duration) Ticker {
	t := Ticker{
		C: make(chan struct{}),
		t: time.NewTicker(period),
	}
	go func() {
		for {
			<-t.t.C
			time.Sleep(time.Duration(
				(rand.Float64()*2 - 1) * float64(variation.Nanoseconds()),
			))
			t.C <- struct{}{}
		}
	}()
	return t
}

func PeriodicDownloader(feed *config.Feed, dstore dstore.DStore, interruptChan <-chan struct{}) {
	log.Print("starting downloader", feed)
	timer := NewTicker(feed.Periodicity, feed.Variation)
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
			log.Print("Stopped feed collection for", feed.ID)
			return
		}
	}
}

type timeGetter func() time.Time

func defaultTimeGetter() time.Time {
	return time.Now().UTC()
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func downloadOnce(feed *config.Feed, dstore dstore.DStore, lastHash storage.Hash, client httpClient, now timeGetter) (*storage.DFile, error) {
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
