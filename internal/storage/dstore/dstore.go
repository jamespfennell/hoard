// Package dstore contains implementations for different DStores used in Hoard
package dstore

import (
	"bytes"
	"errors"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/sirupsen/logrus"
	"io"
	"time"
)

type FlatPersistedDStore struct {
	b persistence.PersistedStorage
}

func NewFlatPersistedDStore(b persistence.PersistedStorage) storage.WritableDStore {
	return FlatPersistedDStore{b: b}
}

func (d FlatPersistedDStore) Store(file storage.DFile, content io.Reader) error {
	return d.b.Put(persistence.Key{Name: file.String()}, content, file.Time)
}

type PersistedDStore struct {
	b   persistence.PersistedStorage
	log logrus.FieldLogger
}

func NewPersistedDStore(b persistence.PersistedStorage, log logrus.FieldLogger) storage.DStore {
	return PersistedDStore{b: b, log: log}
}

func (d PersistedDStore) Store(file storage.DFile, content io.Reader) error {
	return d.b.Put(dFileToPersistenceKey(file), content, file.Time)
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
			d.log.Warnf("Unrecognized directory in persisted storage: %s\n", prefix.Prefix)
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
				d.log.Warnf("Unrecognized file in persisted storage: %s %s\n", searchResult.Prefix, name)
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
