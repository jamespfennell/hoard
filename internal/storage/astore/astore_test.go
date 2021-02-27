package astore

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"testing"
	"time"
)

// fullByteStorageForTesting is a byte storage that has entries for every
// single prefix when searched, with the exception of the empty prefix which
// returns values for every day in 2019 and 2020.
type fullByteStorageForTesting struct {
	// We embed the other byte storage so that we don't have to implement
	// every method
	persistence.InMemoryByteStorage
}

func (byteStorage fullByteStorageForTesting) Search(p persistence.Prefix) ([]persistence.SearchResult, error) {
	prefixes := []persistence.Prefix{p}
	if len(prefixes[0]) == 0 {
		prefixes = extend(prefixes, 2019, 2020)
	}
	if len(prefixes[0]) == 1 {
		prefixes = extend(prefixes, 1, 12)
	}
	if len(prefixes[0]) == 2 {
		prefixes = extend(prefixes, 1, 28)
	}
	if len(prefixes[0]) == 3 {
		prefixes = extend(prefixes, 0, 23)
	}
	var results []persistence.SearchResult
	for _, prefix := range prefixes {
		hour, ok := storage.NewHourFromPersistencePrefix(prefix)
		if !ok {
			panic(fmt.Sprintf("could not parse prefix %v as hour", prefix))
		}
		results = append(
			results,
			persistence.SearchResult{
				Prefix: prefix,
				Names: []string{
					storage.AFile{
						Prefix: "A",
						Hour:   hour,
						Hash:   storage.ExampleHash(),
					}.String(),
				},
			},
		)
	}
	return results, nil
}

func extend(prefixes []persistence.Prefix, start, end int) []persistence.Prefix {
	var newPrefixes []persistence.Prefix
	for _, oldPrefix := range prefixes {
		for i := start; i <= end; i++ {
			var newPrefix persistence.Prefix
			for _, piece := range oldPrefix {
				newPrefix = append(newPrefix, piece)
			}
			newPrefixes = append(
				newPrefixes,
				append(newPrefix, fmt.Sprintf("%02d", i)),
			)
		}
	}
	prefixes = newPrefixes
	return newPrefixes
}

func TestByteStorageBackedAStore_Search(t *testing.T) {
	byteStorage := fullByteStorageForTesting{
		persistence.NewInMemoryBytesStorage(),
	}
	aStore := NewByteStorageBackedAStore(byteStorage)

	noStart := storage.Hour(time.Unix(0, 0))
	testCases := []struct {
		start              storage.Hour
		end                storage.Hour
		numExpectedResults int
	}{
		{
			noStart,
			storage.Date(2021, 1, 0, 0),
			2 * 12 * 28 * 24,
		},
		{
			noStart,
			storage.Date(2020, 2, 3, 4),
			12*28*24 + 28*24 + 2*24 + 5,
		},
		{
			storage.Date(2019, 2, 3, 4),
			storage.Date(2020, 3, 6, 7),
			12*28*24 + 28*24 + 3*24 + 4,
		},
		{
			storage.Date(2019, 11, 3, 4),
			storage.Date(2020, 2, 6, 7),
			3*28*24 + 3*24 + 4,
		},
		{
			storage.Date(2020, 1, 27, 4),
			storage.Date(2020, 2, 6, 7),
			7*24 + 4,
		},
		{
			storage.Date(2020, 2, 3, 4),
			storage.Date(2020, 2, 6, 7),
			3*24 + 4,
		},
		{
			storage.Date(2020, 2, 3, 4),
			storage.Date(2020, 2, 3, 7),
			4,
		},
		{
			storage.Date(2020, 2, 3, 4),
			storage.Date(2020, 2, 3, 4),
			1,
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			var start *storage.Hour
			if testCase.start != noStart {
				start = &testCase.start
			}
			searchResults, err := aStore.Search(start, testCase.end)
			if err != nil {
				t.Errorf("unexpected error in searching: %s", err)
			}
			if len(searchResults) != testCase.numExpectedResults {
				t.Errorf("unexpected number of seach results: %d; expected %d",
					len(searchResults),
					testCase.numExpectedResults,
				)
			}
		})
	}
}
