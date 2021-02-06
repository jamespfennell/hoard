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

	// ListNonEmptyHours() ([]util.Hour, error)

	// ListInHour(hour time.Time) ([]DFile, error)
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

func (dstore *InMemoryDStore) ListNonEmptyHours() ([]util.Hour, error) {
	hours := make(map[util.Hour]struct{})
	for key := range dstore.dFileToContent {
		hours[util.Hour(key.Time.Truncate(time.Hour))] = struct{}{}
	}
	var result []util.Hour
	for hour := range hours {
		result = append(result, hour)
	}
	return result, nil
}

func (dstore *InMemoryDStore) Count() int {
	return len(dstore.dFileToContent)
}

// TODO: match this with the eventual DStore API
func (dstore *InMemoryDStore) Get(file DFile) ([]byte, bool) {
	content, ok := dstore.dFileToContent[file]
	return content, ok
}
