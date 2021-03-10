// Package astore contains implementations for different AStores used in Hoard
package astore

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util"
	"io"
	"strings"
)

type FlatByteStorageAStore struct {
	b persistence.ByteStorage
}

func NewFlatByteStorageAStore(b persistence.ByteStorage) storage.WritableAStore {
	return FlatByteStorageAStore{b: b}
}

func (a FlatByteStorageAStore) Store(file storage.AFile, content []byte) error {
	return a.b.Put(persistence.Key{Name: file.String()}, bytes.NewReader(content))
}

// TODO: PersistedAStore?
type ByteStorageBackedAStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedAStore(b persistence.ByteStorage) storage.AStore {
	return ByteStorageBackedAStore{b: b}
}

func (a ByteStorageBackedAStore) Store(aFile storage.AFile, content []byte) error {
	return a.b.Put(aFileToPersistenceKey(aFile), bytes.NewReader(content))
}

func (a ByteStorageBackedAStore) Get(file storage.AFile) ([]byte, error) {
	readCloser, err := a.b.Get(aFileToPersistenceKey(file))
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(readCloser)
	if err != nil {
		_ = readCloser.Close()
		return nil, err
	}
	return b, readCloser.Close()
}

func (a ByteStorageBackedAStore) Delete(file storage.AFile) error {
	return a.b.Delete(aFileToPersistenceKey(file))
}

func (a ByteStorageBackedAStore) Search(startOpt *hour.Hour, end hour.Hour) ([]storage.SearchResult, error) {
	prefixes := generatePrefixesForSearch(startOpt, end)
	var results []storage.SearchResult
	for _, prefix := range prefixes {
		searchResults, err := a.b.Search(prefix)
		if err != nil {
			return nil, err
		}
		for _, searchResult := range searchResults {
			hr, ok := hour.NewHourFromPersistencePrefix(searchResult.Prefix)
			if !ok {
				fmt.Printf("unrecognized directory in byte storage: %s\n", searchResult.Prefix)
				continue
			}
			result := storage.NewAStoreSearchResult(hr)
			if !hr.IsBetween(startOpt, end) {
				continue
			}
			for _, name := range searchResult.Names {
				aFile, ok := storage.NewAFileFromString(name)
				if !ok {
					fmt.Printf("Unrecognized file in storage: %s\n", name)
					continue
				}
				result.AFiles[aFile] = true
			}
			results = append(results, result)
		}
	}
	return results, nil
}

const maxPerHourPrefixesInSearch = 10
const maxPerDayPrefixesInSearch = 9
const maxPerMonthPrefixesInSearch = 6

// TODO: document this
// TODO: do some very elementary benchmarking
//  downloading all data is faster than downloading 7 hours of data :(
func generatePrefixesForSearch(startOpt *hour.Hour, end hour.Hour) []persistence.Prefix {
	if startOpt == nil {
		return []persistence.Prefix{persistence.EmptyPrefix()}
	}
	start := *startOpt
	var increment int
	var prefixLength int
	numHours := end.Sub(start) + 1
	switch true {
	case numHours <= maxPerHourPrefixesInSearch:
		increment = 1
		prefixLength = 4
	// We subtract 1 from the max constant because N day long periods can
	// span N+1 calender days.
	case (numHours/24 + 1) <= maxPerDayPrefixesInSearch-1:
		increment = 24
		prefixLength = 3
	case numHours/(28*24)+1 <= maxPerMonthPrefixesInSearch-1:
		increment = 28 * 24
		prefixLength = 2
	default:
		increment = 364 * 24
		prefixLength = 1
	}
	// We put the prefixes in a set (essentially) in order to guarantee there
	// are no duplicates. This, along with the constraint that all prefixes
	// returned have the same length, guarantees that there is no prefix
	// search space overlap.
	idToPrefix := map[string]persistence.Prefix{}
	lastPrefixID := end.PersistencePrefix()[:prefixLength].ID()
	for {
		// Keep iterating until the last prefix has been added.
		if _, ok := idToPrefix[lastPrefixID]; ok {
			break
		}
		prefix := start.PersistencePrefix()[:prefixLength]
		idToPrefix[prefix.ID()] = prefix
		start = start.Add(increment)
	}
	prefixes := make([]persistence.Prefix, 0, len(idToPrefix))
	for _, prefix := range idToPrefix {
		prefixes = append(prefixes, prefix)
	}
	return prefixes
}

func (a ByteStorageBackedAStore) String() string {
	return a.b.String()
}

func aFileToPersistenceKey(a storage.AFile) persistence.Key {
	return persistence.Key{
		Prefix: a.Hour.PersistencePrefix(),
		Name:   a.String(),
	}
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
	return nil
}

func (a *InMemoryAStore) Search(startOpt *hour.Hour, end hour.Hour) ([]storage.SearchResult, error) {
	hourToSearchResult := map[hour.Hour]storage.SearchResult{}
	for key := range a.aFileToContent {
		if _, initialized := hourToSearchResult[key.Hour]; !initialized {
			hourToSearchResult[key.Hour] = storage.NewAStoreSearchResult(key.Hour)
		}
		hourToSearchResult[key.Hour].AFiles[key] = true
	}
	var results []storage.SearchResult
	for _, searchResult := range hourToSearchResult {
		if !searchResult.Hour.IsBetween(startOpt, end) {
			continue
		}
		results = append(results, searchResult)
	}
	return results, nil
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

func (m multiAStore) Search(startOpt *hour.Hour, end hour.Hour) ([]storage.SearchResult, error) {
	hourToSearchResult := map[hour.Hour]storage.SearchResult{}
	var errs []error
	for _, aStore := range m.aStores {
		results, err := aStore.Search(startOpt, end)
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			continue
		}
		for _, result := range results {
			if _, initialized := hourToSearchResult[result.Hour]; !initialized {
				hourToSearchResult[result.Hour] = storage.NewAStoreSearchResult(result.Hour)
			}
			for aFile := range result.AFiles {
				hourToSearchResult[result.Hour].AFiles[aFile] = true
			}
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to Search in %d AStore(s): %w",
			len(errs), util.NewMultipleError(errs...))
	}
	var results []storage.SearchResult
	for _, searchResult := range hourToSearchResult {
		results = append(results, searchResult)
	}
	return results, nil
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
