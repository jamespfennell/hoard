// Package dstore contains implementations for different DStores used in Hoard
package dstore

import (
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"time"
)

type ByteStorageBackedDStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedDStore(b persistence.ByteStorage) storage.DStore {
	return ByteStorageBackedDStore{b: b}
}

func (d ByteStorageBackedDStore) Store(file storage.DFile, content []byte) error {
	return d.b.Put(dFileToPersistenceKey(file), content)
}

func (d ByteStorageBackedDStore) Get(file storage.DFile) ([]byte, error) {
	return d.b.Get(dFileToPersistenceKey(file))
}

func (d ByteStorageBackedDStore) Delete(file storage.DFile) error {
	return d.b.Delete(dFileToPersistenceKey(file))
}

func (d ByteStorageBackedDStore) ListNonEmptyHours() ([]storage.Hour, error) {
	prefixes, err := d.b.Search()
	if err != nil {
		return nil, err
	}
	var hours []storage.Hour
	for _, prefix := range prefixes {
		hour, ok := storage.NewHourFromPersistencePrefix(prefix.Prefix)
		if !ok {
			fmt.Printf("unrecognized directory in byte storage: %s\n", prefix.Prefix)
			continue
		}
		hours = append(hours, hour)
	}
	return hours, nil
}

func (d ByteStorageBackedDStore) ListInHour(hour storage.Hour) ([]storage.DFile, error) {
	p := hour.PersistencePrefix()
	keys, err := d.b.List(p)
	if err != nil {
		return nil, err
	}
	var dFiles []storage.DFile
	for _, key := range keys {
		dFile, ok := storage.NewDFileFromString(key.Name)
		if !ok {
			fmt.Printf("Unrecognized file: %s\n", key.Name)
			continue
		}
		dFiles = append(dFiles, dFile)
	}
	return dFiles, nil
}

func dFileToPersistenceKey(d storage.DFile) persistence.Key {
	return persistence.Key{
		Prefix: storage.Hour(d.Time).PersistencePrefix(),
		Name:   d.String(),
	}
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
	var result []storage.DFile
	for dFile, _ := range dstore.dFileToContent {
		if storage.Hour(dFile.Time.Truncate(time.Hour)) == hour {
			result = append(result, dFile)
		}
	}
	return result, nil
}

func (dstore *InMemoryDStore) Count() int {
	return len(dstore.dFileToContent)
}
