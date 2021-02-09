package dstore

import (
	"errors"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"time"
)

type DStore interface {
	Store(dFile storage.DFile, content []byte) error

	Get(dFile storage.DFile) ([]byte, error)

	Delete(dFile storage.DFile) error

	// Lists all hours for which there is at least 1 DFile whose time is within that hour
	ListNonEmptyHours() ([]storage.Hour, error)

	ListInHour(hour storage.Hour) ([]storage.DFile, error)
}

// TODO: non public?
type ByteStorageBackedDStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedDStore(b persistence.ByteStorage) DStore {
	return ByteStorageBackedDStore{b: b}
}

func (d ByteStorageBackedDStore) Store(file storage.DFile, content []byte) error {
	return d.b.Put(storage.DFileToPersistenceKey(file), content)
}

func (d ByteStorageBackedDStore) Get(file storage.DFile) ([]byte, error) {
	return d.b.Get(storage.DFileToPersistenceKey(file))
}

func (d ByteStorageBackedDStore) Delete(file storage.DFile) error {
	return d.b.Delete(storage.DFileToPersistenceKey(file))
}

func (d ByteStorageBackedDStore) ListNonEmptyHours() ([]storage.Hour, error) {
	prefixes, err := d.b.Search()
	if err != nil {
		return nil, err
	}
	var hours []storage.Hour
	for _, prefix := range prefixes {
		hour, ok := storage.PersistencePrefixToHour(prefix)
		if !ok {
			// TODO: move this prefix to trash
			continue
		}
		hours = append(hours, hour)
	}
	return hours, nil
}

func (d ByteStorageBackedDStore) ListInHour(hour storage.Hour) ([]storage.DFile, error) {
	p := storage.TimeToPersistencePrefix(time.Time(hour))
	keys, err := d.b.List(p)
	if err != nil {
		return nil, err
	}
	var dfiles []storage.DFile
	for _, key := range keys {
		dfile, ok := storage.PersistenceKeyToDFile(key)
		if !ok {
			// TODO: move this key to trash
			continue
		}
		dfiles = append(dfiles, dfile)
	}
	return dfiles, nil
}

type InMemoryDStore struct {
	dFileToContent map[storage.DFile][]byte
}

func NewInMemoryDStore() *InMemoryDStore {
	return &InMemoryDStore{
		dFileToContent: make(map[storage.DFile][]byte),
	}
}

func (dstore *InMemoryDStore) Store(file storage.DFile, content []byte) error {
	dstore.dFileToContent[file] = content
	return nil
}

func (dstore *InMemoryDStore) Get(dFile storage.DFile) ([]byte, error) {
	content, ok := dstore.dFileToContent[dFile]
	if !ok {
		return nil, errors.New("no such DFile")
	}
	return content, nil
}

func (dstore *InMemoryDStore) Delete(dFile storage.DFile) error {
	return errors.New("not implemented 2")
}

func (dstore *InMemoryDStore) ListNonEmptyHours() ([]storage.Hour, error) {
	hours := make(map[storage.Hour]struct{})
	for key := range dstore.dFileToContent {
		hours[storage.Hour(key.Time.Truncate(time.Hour))] = struct{}{}
	}
	var result []storage.Hour
	for hour := range hours {
		result = append(result, hour)
	}
	return result, nil
}
func (dstore *InMemoryDStore) ListInHour(hour storage.Hour) ([]storage.DFile, error) {
	return nil, errors.New("not implemented 1")
}

func (dstore *InMemoryDStore) Count() int {
	return len(dstore.dFileToContent)
}