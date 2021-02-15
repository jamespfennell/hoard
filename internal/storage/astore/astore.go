// Package astore contains implementations for different AStores used in Hoard
package astore

import (
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"strconv"
	"strings"
	"time"
)

type ByteStorageBackedAStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedAStore(b persistence.ByteStorage) storage.AStore {
	return ByteStorageBackedAStore{b: b}
}

func (a ByteStorageBackedAStore) Store(aFile storage.AFile, content []byte) error {
	return a.b.Put(aFileToPersistenceKey(aFile), content)
}

func (a ByteStorageBackedAStore) Get(file storage.AFile) ([]byte, error) {
	return a.b.Get(aFileToPersistenceKey(file))
}

func (a ByteStorageBackedAStore) Delete(file storage.AFile) error {
	return a.b.Delete(aFileToPersistenceKey(file))
}

func (a ByteStorageBackedAStore) ListNonEmptyHours() ([]storage.Hour, error) {
	prefixes, err := a.b.Search()
	if err != nil {
		return nil, err
	}
	var hours []storage.Hour
	for _, prefix := range prefixes {
		hour, ok := persistencePrefixToHour(prefix)
		if !ok {
			// TODO: log and move this prefix to trash
			continue
		}
		hours = append(hours, hour)
	}
	return hours, nil
}

func (a ByteStorageBackedAStore) ListInHour(hour storage.Hour) ([]storage.AFile, error) {
	p := hourToPersistencePrefix(hour)
	keys, err := a.b.List(p)
	if err != nil {
		return nil, err
	}
	var aFiles []storage.AFile
	for _, key := range keys {
		aFile, ok := storage.NewAFileFromString(key.Name)
		if !ok {
			fmt.Println("no match", key.Name)
			// TODO: log and move this key to trash
			continue
		}
		// TODO: verify that the prefix also matches
		aFiles = append(aFiles, aFile)
	}
	return aFiles, nil
}

// TODO: this is just the String function...?
func aFileToPersistenceKey(a storage.AFile) persistence.Key {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(a.Prefix)
	nameBuilder.WriteString(storage.ISO8601Hour(a.Time))
	nameBuilder.WriteString("_")
	nameBuilder.WriteString(string(a.Hash))
	nameBuilder.WriteString(".tar.gz")
	return persistence.Key{
		Prefix: hourToPersistencePrefix(a.Time),
		Name:   nameBuilder.String(),
	}
}

func hourToPersistencePrefix(h storage.Hour) persistence.Prefix {
	t := time.Time(h)
	return []string{
		formatInt(t.Year()),
		formatInt(int(t.Month())),
		formatInt(t.Day()),
		formatInt(t.Hour()),
	}
}

// TODO: dedup between here an dstore?
func persistencePrefixToHour(p persistence.Prefix) (storage.Hour, bool) {
	if len(p) != 4 {
		return storage.Hour{}, false
	}
	t, err := time.Parse("2006-01-02-15", strings.Join(p, "-"))
	if err != nil {
		return storage.Hour{}, false
	}
	return storage.Hour(t), true
}

func formatInt(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}

type InMemoryAStore struct {
	aFileToContent map[storage.AFile][]byte
}

func NewInMemoryAStore() *InMemoryAStore {
	return &InMemoryAStore{
		aFileToContent: make(map[storage.AFile][]byte),
	}
}

func (a *InMemoryAStore) Store(aFile storage.AFile, content []byte) error {
	a.aFileToContent[aFile] = content
	return nil
}

func (a *InMemoryAStore) Get(aFile storage.AFile) ([]byte, error) {
	content, ok := a.aFileToContent[aFile]
	if !ok {
		return nil, errors.New("no such AFile")
	}
	return content, nil
}

func (a *InMemoryAStore) Delete(file storage.AFile) error {
	delete(a.aFileToContent, file)
	// TODO: error if doesn't exist?
	return nil
}

func (a *InMemoryAStore) ListNonEmptyHours() ([]storage.Hour, error) {
	hours := make(map[storage.Hour]struct{})
	for key := range a.aFileToContent {
		hours[key.Time] = struct{}{}
	}
	var result []storage.Hour
	for hour := range hours {
		result = append(result, hour)
	}
	return result, nil
}

func (a *InMemoryAStore) ListInHour(hour storage.Hour) ([]storage.AFile, error) {
	var result []storage.AFile
	for aFile, _ := range a.aFileToContent {
		if aFile.Time == hour {
			result = append(result, aFile)
		}
	}
	return result, nil
}
