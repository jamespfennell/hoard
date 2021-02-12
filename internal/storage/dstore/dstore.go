package dstore

import (
	"errors"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"strconv"
	"strings"
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
		hour, ok := persistencePrefixToHour(prefix)
		if !ok {
			// TODO: log and move this prefix to trash
			continue
		}
		hours = append(hours, hour)
	}
	return hours, nil
}

func (d ByteStorageBackedDStore) ListInHour(hour storage.Hour) ([]storage.DFile, error) {
	p := timeToPersistencePrefix(time.Time(hour))
	keys, err := d.b.List(p)
	if err != nil {
		return nil, err
	}
	var dFiles []storage.DFile
	for _, key := range keys {
		dFile, ok := storage.NewDFileFromString(key.Name)
		if !ok {
			// TODO: log and move this key to trash
			continue
		}
		// TODO: verify that the prefix also matches
		dFiles = append(dFiles, dFile)
	}
	return dFiles, nil
}

func dFileToPersistenceKey(d storage.DFile) persistence.Key {
	return persistence.Key{
		Prefix: timeToPersistencePrefix(d.Time),
		Name:   d.String(),
	}
}

func timeToPersistencePrefix(t time.Time) persistence.Prefix {
	return []string{
		formatInt(t.Year()),
		formatInt(int(t.Month())),
		formatInt(t.Day()),
		formatInt(t.Hour()),
	}
}

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
