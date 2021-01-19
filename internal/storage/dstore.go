package storage

import (
	"github.com/jamespfennell/hoard/internal/storage/util"
	"time"
)

type DFile struct {
	Prefix  string
	Postfix string
	Time    time.Time
	Hash    util.Hash
}

type DStore interface {
	StoreDFile(dFile DFile, content []byte) error

	// ListNonEmptyHours() ([]time.Time, error)

	// ListDFilesForHour(hour time.Time) ([]DFile, error)
}

type InMemoryDStore struct {
	dFileToContent map[DFile][]byte
}

func NewInMemoryDStore() *InMemoryDStore {
	return &InMemoryDStore{
		dFileToContent: make(map[DFile][]byte),
	}
}

func (dstore *InMemoryDStore) StoreDFile(file DFile, content []byte) error {
	dstore.dFileToContent[file] = content
	return nil
}

func (dstore *InMemoryDStore) Count() int {
	return len(dstore.dFileToContent)
}

// TODO: match this with the eventual DStore API
func (dstore *InMemoryDStore) Get(file DFile) ([]byte, bool) {
	content, ok := dstore.dFileToContent[file]
	return content, ok
}
