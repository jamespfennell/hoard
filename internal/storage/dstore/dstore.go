// Package dstore contains implementations for different DStores used in Hoard
package dstore

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"io"
	"os"
	"time"
)

type FlatPersistedDStore struct {
	b persistence.PersistedStorage
}

func NewFlatPersistedDStore(b persistence.PersistedStorage) storage.WritableDStore {
	return FlatPersistedDStore{b: b}
}

func (d FlatPersistedDStore) Store(file storage.DFile, content io.Reader) error {
	return d.b.Put(persistence.Key{Name: file.String()}, content)
}

type PersistedDStore struct {
	b persistence.PersistedStorage
}

func NewPersistedDStore(b persistence.PersistedStorage) storage.DStore {
	return PersistedDStore{b: b}
}

func (d PersistedDStore) Store(file storage.DFile, content io.Reader) error {
	return d.b.Put(dFileToPersistenceKey(file), content)
}

func (d PersistedDStore) Get(file storage.DFile) (io.ReadCloser, error) {
	return d.b.Get(dFileToPersistenceKey(file))
}

func (d PersistedDStore) Delete(file storage.DFile) error {
	return d.b.Delete(dFileToPersistenceKey(file))
}

func (d PersistedDStore) ListNonEmptyHours() ([]hour.Hour, error) {
	prefixes, err := d.b.Search(persistence.EmptyPrefix())
	if err != nil {
		return nil, err
	}
	var hours []hour.Hour
	for _, prefix := range prefixes {
		hr, ok := hour.NewHourFromPersistencePrefix(prefix.Prefix)
		if !ok {
			fmt.Printf("unrecognized directory in byte storage: %s\n", prefix.Prefix)
			continue
		}
		hours = append(hours, hr)
	}
	return hours, nil
}

func (d PersistedDStore) ListInHour(hour hour.Hour) ([]storage.DFile, error) {
	p := hour.PersistencePrefix()
	searchResults, err := d.b.Search(p)
	if err != nil {
		return nil, err
	}
	var dFiles []storage.DFile
	for _, searchResult := range searchResults {
		for _, name := range searchResult.Names {
			dFile, ok := storage.NewDFileFromString(name)
			if !ok {
				fmt.Printf("Unrecognized file: %s\n", name)
				continue
			}
			dFiles = append(dFiles, dFile)
		}
	}
	return dFiles, nil
}

func dFileToPersistenceKey(d storage.DFile) persistence.Key {
	return persistence.Key{
		Prefix: timeToHour(d.Time).PersistencePrefix(),
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

func (dstore *InMemoryDStore) Store(file storage.DFile, content io.Reader) error {
	b, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	dstore.dFileToContent[file] = b
	return nil
}

func (dstore *InMemoryDStore) Get(dFile storage.DFile) (io.ReadCloser, error) {
	content, ok := dstore.dFileToContent[dFile]
	if !ok {
		return nil, errors.New("no such DFile")
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (dstore *InMemoryDStore) Delete(storage.DFile) error {
	return errors.New("InMemoryDStore#Delete not implemented")
}

func (dstore *InMemoryDStore) ListNonEmptyHours() ([]hour.Hour, error) {
	hours := make(map[hour.Hour]struct{})
	for key := range dstore.dFileToContent {
		hours[timeToHour(key.Time)] = struct{}{}
	}
	var result []hour.Hour
	for hr := range hours {
		result = append(result, hr)
	}
	return result, nil
}

func (dstore *InMemoryDStore) ListInHour(hr hour.Hour) ([]storage.DFile, error) {
	var result []storage.DFile
	for dFile := range dstore.dFileToContent {
		if timeToHour(dFile.Time) == hr {
			result = append(result, dFile)
		}
	}
	return result, nil
}

func (dstore *InMemoryDStore) Count() int {
	return len(dstore.dFileToContent)
}

func timeToHour(t time.Time) hour.Hour {
	return hour.Date(t.Year(), t.Month(), t.Day(), t.Hour())
}

type persistedDStoreFactory struct {
	root string
}

func NewPersistedDStoreFactory(root string) storage.DStoreFactory {
	return &persistedDStoreFactory{root}
}

func (factory *persistedDStoreFactory) New() (storage.DStore, func()) {
	tmpDir, err := os.MkdirTemp(factory.root, "")
	if err != nil {
		fmt.Printf("Failed to create temporary on disk DStore: %s\nFalling back in in-memory\n", err)
		return NewInMemoryDStore(), func() {}
	}
	return NewPersistedDStore(persistence.NewDiskPersistedStorage(tmpDir)), func() {
		_ = os.RemoveAll(tmpDir)
	}
}

type inMemoryDStoreFactory struct{}

func NewInMemoryDStoreFactory() storage.DStoreFactory {
	return inMemoryDStoreFactory{}
}

func (factory inMemoryDStoreFactory) New() (storage.DStore, func()) {
	return NewInMemoryDStore(), func() {}
}
