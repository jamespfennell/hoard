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

type FlatPersistedAStore struct {
	b persistence.PersistedStorage
}

func NewFlatPersistedAStore(b persistence.PersistedStorage) storage.WritableAStore {
	return FlatPersistedAStore{b: b}
}

func (a FlatPersistedAStore) Store(file storage.AFile, reader io.Reader) error {
	return a.b.Put(persistence.Key{Name: file.String()}, reader)
}

type PersistedAStore struct {
	b persistence.PersistedStorage
}

func NewPersistedAStore(b persistence.PersistedStorage) storage.AStore {
	return PersistedAStore{b: b}
}

func (a PersistedAStore) Store(aFile storage.AFile, reader io.Reader) error {
	return a.b.Put(aFileToPersistenceKey(aFile), reader)
}

func (a PersistedAStore) Get(file storage.AFile) (io.ReadCloser, error) {
	r, err := a.b.Get(aFileToPersistenceKey(file))
	if err != nil {
		r, err = a.b.Get(aFileToLegacyPersistenceKey(file))
	}
	return r, err
}

func (a PersistedAStore) Delete(file storage.AFile) error {
	return util.NewMultipleError(
		a.b.Delete(aFileToPersistenceKey(file)),
		a.b.Delete(aFileToLegacyPersistenceKey(file)))
}

func (a PersistedAStore) Search(startOpt *hour.Hour, end hour.Hour) ([]storage.SearchResult, error) {
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

func (a PersistedAStore) String() string {
	return a.b.String()
}

func aFileToPersistenceKey(a storage.AFile) persistence.Key {
	return persistence.Key{
		Prefix: a.Hour.PersistencePrefix(),
		Name:   a.String(),
	}
}

func aFileToLegacyPersistenceKey(a storage.AFile) persistence.Key {
	return persistence.Key{
		Prefix: a.Hour.PersistencePrefix(),
		Name:   a.LegacyString(),
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

func (a *InMemoryAStore) Store(aFile storage.AFile, reader io.Reader) error {
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	a.aFileToContent[aFile] = content
	return nil
}

func (a *InMemoryAStore) Get(aFile storage.AFile) (io.ReadCloser, error) {
	content, ok := a.aFileToContent[aFile]
	if !ok {
		return nil, errors.New("no such AFile")
	}
	return io.NopCloser(bytes.NewReader(content)), nil
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
type ReplicatedAStore struct {
	aStores []storage.AStore
}

func NewReplicatedAStore(aStores ...storage.AStore) ReplicatedAStore {
	return ReplicatedAStore{aStores: aStores}
}

func (m ReplicatedAStore) Store(aFile storage.AFile, reader io.Reader) error {
	// TODO: is there a better way here?
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	var errs []error
	for _, aStore := range m.aStores {
		err := aStore.Store(aFile, bytes.NewReader(content))
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

func (m ReplicatedAStore) Get(aFile storage.AFile) (io.ReadCloser, error) {
	var errs []error
	for _, aStore := range m.aStores {
		b, err := aStore.Get(aFile)
		if err == nil {
			return b, err
		}
		if b != nil {
			_ = b.Close()
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("failed to retrive from any AStore: %w",
		util.NewMultipleError(errs...))
}

func (m ReplicatedAStore) Search(startOpt *hour.Hour, end hour.Hour) ([]storage.SearchResult, error) {
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

func (m ReplicatedAStore) Delete(aFile storage.AFile) error {
	var errs []error
	for _, aStore := range m.aStores {
		errs = append(errs, aStore.Delete(aFile))
	}
	return util.NewMultipleError(errs...)
}

func (m ReplicatedAStore) String() string {
	var aStoreStrings []string
	for _, aStore := range m.aStores {
		aStoreStrings = append(aStoreStrings, aStore.String())
	}
	return fmt.Sprintf("storage with %d replicas: %s",
		len(m.aStores), strings.Join(aStoreStrings, ", "))
}

func (m ReplicatedAStore) Replicas() []storage.AStore {
	return m.aStores
}
