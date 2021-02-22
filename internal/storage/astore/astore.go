// Package astore contains implementations for different AStores used in Hoard
package astore

import (
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util"
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

func (a ByteStorageBackedAStore) ListNonEmptyHours() ([]storage.SearchResult, error) {
	nonEmptyPrefixes, err := a.b.Search()
	if err != nil {
		return nil, err
	}
	var results []storage.SearchResult
	for _, nonEmptyPrefix := range nonEmptyPrefixes {
		hour, ok := persistencePrefixToHour(nonEmptyPrefix.Prefix)
		if !ok {
			// TODO: log and move this prefix to trash
			continue
		}
		result := storage.NewSearchResult(hour)
		for _, name := range nonEmptyPrefix.Names {
			result.Add(name)
		}
		results = append(results, result)
	}
	return results, nil
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

func (a ByteStorageBackedAStore) String() string {
	return a.b.String()
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

func (a *InMemoryAStore) ListNonEmptyHours() ([]storage.SearchResult, error) {
	hourToSearchResult := map[storage.Hour]storage.SearchResult{}
	for key := range a.aFileToContent {
		if _, initialized := hourToSearchResult[key.Time]; !initialized {
			hourToSearchResult[key.Time] = storage.NewSearchResult(key.Time)
		}
		hourToSearchResult[key.Time].Add(string(key.Hash))
	}
	var results []storage.SearchResult
	for _, searchResult := range hourToSearchResult {
		results = append(results, searchResult)
	}
	return results, nil
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

func (a *InMemoryAStore) String() string {
	return "in memory"
}

// TODO: write tests for this
type multiAStore struct {
	aStores []storage.AStore
}

func NewMultiAStore(aStores ...storage.AStore) storage.AStore {
	if len(aStores) == 1 {
		return aStores[0]
	}
	return multiAStore{aStores: aStores}
}

func (m multiAStore) Store(aFile storage.AFile, content []byte) error {
	var errs []error
	for _, aStore := range m.aStores {
		err := aStore.Store(aFile, content)
		if err != nil {

			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("failed to store in %d AStore(s): %w",
		len(errs), util.NewMultipleError(errs...))
}

func (m multiAStore) Get(aFile storage.AFile) ([]byte, error) {
	var errs []error
	for _, aStore := range m.aStores {
		b, err := aStore.Get(aFile)
		if err == nil {
			return b, err
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("failed to retrive from any AStore: %w",
		util.NewMultipleError(errs...))
}

func (m multiAStore) ListNonEmptyHours() ([]storage.SearchResult, error) {
	hourToSearchResult := map[storage.Hour]storage.SearchResult{}
	var errs []error
	for _, aStore := range m.aStores {
		results, err := aStore.ListNonEmptyHours()
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			continue
		}
		for _, result := range results {
			if _, initialized := hourToSearchResult[result.Hour()]; !initialized {
				hourToSearchResult[result.Hour()] = storage.NewSearchResult(result.Hour())
			}
			hourToSearchResult[result.Hour()].AddAll(result)
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to ListNonEmptyHours in %d AStore(s): %w",
			len(errs), util.NewMultipleError(errs...))
	}
	var results []storage.SearchResult
	for _, searchResult := range hourToSearchResult {
		results = append(results, searchResult)
	}
	return results, nil
}

func (m multiAStore) ListInHour(hour storage.Hour) ([]storage.AFile, error) {
	aFiles := map[storage.AFile]struct{}{}
	var errs []error
	for _, aStore := range m.aStores {
		thisAFiles, err := aStore.ListInHour(hour)
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			continue
		}
		for _, aFile := range thisAFiles {
			aFiles[aFile] = struct{}{}
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to ListInHour in %d AStore(s): %w",
			len(errs), util.NewMultipleError(errs...))
	}
	var result []storage.AFile
	for aFile := range aFiles {
		result = append(result, aFile)
	}
	return result, nil
}

func (m multiAStore) Delete(aFile storage.AFile) error {
	var errs []error
	for _, aStore := range m.aStores {
		errs = append(errs, aStore.Delete(aFile))
	}
	return util.NewMultipleError(errs...)
}

func (m multiAStore) String() string {
	var aStoreStrings []string
	for _, aStore := range m.aStores {
		aStoreStrings = append(aStoreStrings, aStore.String())
	}
	return fmt.Sprintf("storage with %d replicas: %s",
		len(m.aStores), strings.Join(aStoreStrings, ", "))
}
