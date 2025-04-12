package astore

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
)

// fullByteStorageForTesting is a byte storage that has entries for every
// single prefix when searched, with the exception of the empty prefix which
// returns values for every day in 2019 and 2020.
type fullByteStorageForTesting struct {
	// We embed the other byte storage so that we don't have to implement
	// every method
	*persistence.InMemoryPersistedStorage

	numSearches int
}

func (byteStorage *fullByteStorageForTesting) Search(p persistence.Prefix) ([]persistence.SearchResult, error) {
	byteStorage.numSearches++
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
		hr, ok := hour.NewHourFromPersistencePrefix(prefix)
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
						Hour:   hr,
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
			newPrefix = append(newPrefix, oldPrefix...)
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

	noStart := hour.Date(0, 0, 0, 0)
	testCases := []struct {
		start               hour.Hour
		end                 hour.Hour
		numExpectedResults  int
		numExpectedSearches int // number of underlying byte storage searches
	}{
		// No start time searches
		{
			noStart,
			hour.Date(2021, 1, 0, 0),
			2 * 12 * 28 * 24,
			1,
		},
		{
			noStart,
			hour.Date(2020, 2, 3, 4),
			12*28*24 + 28*24 + 2*24 + 5,
			1,
		},
		// Searches with a length of hours
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3, 4),
			1,
			1,
		},
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3, 7),
			4,
			4,
		},
		// Edge cases when the search transitions from per-hour to per-day
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3, 4+maxPerHourPrefixesInSearch-1),
			maxPerHourPrefixesInSearch,
			maxPerHourPrefixesInSearch,
		},
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3, 4+maxPerHourPrefixesInSearch),
			maxPerHourPrefixesInSearch + 1,
			1,
		},
		{
			hour.Date(2020, 2, 3, 16),
			hour.Date(2020, 2, 3, 16+maxPerHourPrefixesInSearch),
			maxPerHourPrefixesInSearch + 1,
			2,
		},
		// Searches with a length of days
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 6, 7),
			3*24 + 4,
			4,
		},
		// Edge cases when the search transitions from per-day to per-month
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3+maxPerDayPrefixesInSearch-1, 2),
			(maxPerDayPrefixesInSearch-1)*24 - 1,
			maxPerDayPrefixesInSearch,
		},
		{
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3+maxPerDayPrefixesInSearch-1, 3),
			(maxPerDayPrefixesInSearch - 1) * 24,
			1,
		}, {
			hour.Date(2020, 2, 3, 4),
			hour.Date(2020, 2, 3+maxPerDayPrefixesInSearch-1, 4),
			(maxPerDayPrefixesInSearch-1)*24 + 1,
			1,
		},
		// Searches with a length of months
		{
			hour.Date(2020, 4, 3, 4),
			hour.Date(2020, 6, 6, 7),
			2*28*24 + 3*24 + 4,
			3,
		},
		// Edge cases when the search transitions from per-month to per-year
		{
			hour.Date(2019, 2, 3, 4),
			hour.Date(2019, 6, 3, 2),
			4*24*28 - 1,
			5,
		},
		{
			hour.Date(2019, 2, 3, 4),
			hour.Date(2019, 8, 3, 2),
			6*24*28 - 1,
			1,
		},
		{
			hour.Date(2019, 2, 3, 4),
			hour.Date(2019, 8, 3, 3),
			6 * 24 * 28,
			1,
		},
		// Searches with a length of years
		{
			hour.Date(2019, 2, 3, 4),
			hour.Date(2020, 11, 3, 4),
			12*28*24 + 9*28*24 + 1,
			2,
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			byteStorage := fullByteStorageForTesting{
				persistence.NewInMemoryPersistedStorage(),
				0,
			}
			aStore := NewPersistedAStore(&byteStorage, slog.Default())

			var start *hour.Hour
			if testCase.start != noStart {
				start = &testCase.start
			}

			searchResults, err := aStore.Search(start, testCase.end)

			if byteStorage.numSearches != testCase.numExpectedSearches {
				t.Errorf("unexpected number of searches: %d; expected %d",
					byteStorage.numSearches, testCase.numExpectedSearches)
			}
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
