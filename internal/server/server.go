package server

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/download"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"log"
	"path"
	"sync"
)

// TODO: just use a closed channel instead
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

func Run(c config.Config, workspaceRoot string, port int, interruptChan <-chan struct{}) {
	propagator := NewInterruptPropagator(interruptChan)
	var w sync.WaitGroup
	for _, feed := range c.Feeds {
		w.Add(1)
		dstore := persistence.NewKVBackedDStore(
			persistence.NewOnDiskKVStore(path.Join(workspaceRoot,"downloads", feed.ID)),
			)
		feed := feed
		go func() {
			download.PeriodicDownloader(&feed, dstore, propagator.AddTarget())
			w.Done()
		}()
	}
	w.Wait()
	log.Print("Stopping Hoard server")
}




