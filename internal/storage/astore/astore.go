// Package astore contains implementations for different AStores used in Hoard
package astore

import (
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util"
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

func (a ByteStorageBackedAStore) Search(startOpt *storage.Hour, end storage.Hour) ([]storage.SearchResult, error) {
	prefixes := generatePrefixesForSearch(startOpt, end)
	var results []storage.SearchResult
	for _, prefix := range prefixes {
		searchResults, err := a.b.Search(prefix)
		if err != nil {
			return nil, err
		}
		for _, searchResult := range searchResults {
			hour, ok := storage.NewHourFromPersistencePrefix(searchResult.Prefix)
			if !ok {
				fmt.Printf("unrecognized directory in byte storage: %s\n", searchResult.Prefix)
				continue
			}
			result := storage.NewAStoreSearchResult(hour)
			if !hour.IsBetween(startOpt, end) {
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

func generatePrefixesForSearch(startOpt *storage.Hour, end storage.Hour) []persistence.Prefix {
	if startOpt == nil {
		return []persistence.Prefix{persistence.EmptyPrefix()}
	}
	start := *startOpt
	// We put the prefixes in a set (essentially) in order to guarantee there
	// are no duplicates. This, along with the constraint that all prefixes
	// returned have the same length, guarantees that there is no prefix overlap.
	idToPrefix := map[string]persistence.Prefix{}
	numHours := time.Time(end).Sub(time.Time(start))/time.Hour + 1
	// TODO: test all the edge cases when the regime transitions from
	//  per hour to per day etc
	switch true {
	case numHours < 10: // less than 10 hours
		// TODO: extract this logic
		for i := 0; i < int(numHours); i++ {
			prefix := start.PersistencePrefix()
			idToPrefix[prefix.ID()] = prefix
			start = storage.Hour(time.Time(start).Add(time.Hour))
		}
	// generate the up to 10 hour prefixes
	case numHours/24 < 9: // less than 9 days. The hours involved can span 10 calendar days
		// generate the up to 10 day prefixes?
		for i := 0; i < int(numHours/24)+1; i++ {
			prefix := start.PersistencePrefix()[:3]
			idToPrefix[prefix.ID()] = prefix
			start = storage.Hour(time.Time(start).Add(24 * time.Hour))
		}
	// TODO there is an edge case here in the equality case, test it?
	case numHours/(28*24) < 6: // less than 6 months. The hours involved can span 7 calendar months
		// generate the up to 7 month prefixes
		for i := 0; i <= int(numHours/(24*28))+1; i++ {
			prefix := start.PersistencePrefix()[:2]
			idToPrefix[prefix.ID()] = prefix
			start = storage.Hour(time.Time(start).Add(28 * 24 * time.Hour))
		}
	default:
		startYear := time.Time(start).Year()
		endYear := time.Time(end).Year()
		for year := startYear; year <= endYear; year++ {
			prefix := storage.Date(year, 6, 6, 6).PersistencePrefix()[:1]
			idToPrefix[prefix.ID()] = prefix
		}
		// generate per year prefixes for every year

	}
	// TODO: initialize with capcatiy
	var prefixes []persistence.Prefix
	for _, prefix := range idToPrefix {
		prefixes = append(prefixes, prefix)
	}
	return prefixes
}

// TODO: destroy
func (a ByteStorageBackedAStore) ListInHour(hour storage.Hour) ([]storage.AFile, error) {
	p := hour.PersistencePrefix()
	keys, err := a.b.List(p)
	if err != nil {
		return nil, err
	}
	var aFiles []storage.AFile
	for _, key := range keys {
		aFile, ok := storage.NewAFileFromString(key.Name)
		if !ok {
			fmt.Printf("Unrecognized file in storage: %s\n", key.Name)
			continue
		}
		aFiles = append(aFiles, aFile)
	}
	return aFiles, nil
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

func (a *InMemoryAStore) Search(startOpt *storage.Hour, end storage.Hour) ([]storage.SearchResult, error) {
	hourToSearchResult := map[storage.Hour]storage.SearchResult{}
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

func (a *InMemoryAStore) ListInHour(hour storage.Hour) ([]storage.AFile, error) {
	var result []storage.AFile
	for aFile, _ := range a.aFileToContent {
		if aFile.Hour == hour {
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

func (m multiAStore) Search(startOpt *storage.Hour, end storage.Hour) ([]storage.SearchResult, error) {
	hourToSearchResult := map[storage.Hour]storage.SearchResult{}
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
