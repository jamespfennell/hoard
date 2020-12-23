package server

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type InterruptPropagator struct {
	source <-chan struct{}
	targets []chan<- struct{}
}

func NewInterruptPropagator(source <-chan struct{}) *InterruptPropagator {
	p := InterruptPropagator{source: source}
	go p.run()
	return &p
}

func (p *InterruptPropagator) AddTarget() <-chan struct{} {
	t := make(chan struct{}, 1)
	p.targets = append(p.targets, t)
	return t
}

func (p *InterruptPropagator) run() {
	<-p.source
	for _, target := range p.targets {
		target <- struct{}{}
	}
}

func Run(c config.Config, workspacePath string, port int, interruptChan <-chan struct{}) {
	propagator := NewInterruptPropagator(interruptChan)
	workspace := storage.NewWorkspace(workspacePath)
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		w.Add(1)
		feed := feed
		go func() {
			collectFeed(&feed, &workspace, propagator.AddTarget())
			w.Done()
		}()
	}
	w.Wait()
	log.Print("Stopping Hoard server")
}




const encodeStd = "abcdefghijklmnopqrstuvwxyz234567"

func CalculateHash(b []byte) (string, error) {
	h := sha256.New()
	_, err := h.Write(b)
	if err != nil {
		return "", err
	}
	return base32.NewEncoding(encodeStd).EncodeToString(h.Sum(nil))[:12], nil
}

func collectFeed(feed *config.Feed, workspace storage.ADStore, interruptChan <-chan struct{}) {
	log.Print("starting", feed)
	timer := time.NewTicker(feed.Periodicity)
	var lastHash string
	for {
		select {
		case <-timer.C:
			dFile, err := downloadFeed(feed, workspace, lastHash)
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

func downloadFeed(feed *config.Feed, workspace storage.ADStore, lastHash string) (*storage.DFile, error) {
	resp, err := http.Get(feed.URL)
	if err != nil {
		return nil, err
	}
	// TODO: handle the error
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	hash, err := CalculateHash(bytes)
	if err != nil {
		return nil, err
	}
	dFile := storage.DFile{
		Feed: feed,
		Time: time.Now().UTC(),
		Hash: hash,
	}
	if hash == lastHash {
		// TODO: don't skip if this is a new hour
		return &dFile, nil
	}
	return &dFile, workspace.StoreDFile(dFile, bytes)
}

func archive(feed *config.Feed, workspace storage.ADStore, interruptChan <-chan struct{}) error {
	hours, err := workspace.ListNonEmptyHours(feed)
	if err != nil {
		return err
	}

}