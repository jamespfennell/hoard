package testutil

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"io"
	"reflect"
	"testing"
	"time"
)

var Data = []struct {
	Content []byte
	DFile   storage.DFile
	Hour    hour.Hour
}{
	{
		[]byte{50, 51, 52},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{50, 51, 52}),
		},
		hour.Date(2000, 1, 2, 3),
	},
	{
		[]byte{60, 61, 62},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 5, 5, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{60, 61, 62}),
		},
		hour.Date(2000, 1, 2, 3),
	},
	{
		[]byte{60, 61, 62},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 6, 10, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{60, 61, 62}),
		},
		hour.Date(2000, 1, 2, 3),
	},
	{
		[]byte{70, 71, 72},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 6, 15, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{70, 71, 72}),
		},
		hour.Date(2000, 1, 2, 3),
	},
}

func ErrorOrFail(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Unexpected error '%s'", err)
	}
}

func DStoreHasDFile(dStore storage.ReadableDStore, dFile storage.DFile, expectedContent []byte) error {
	reader, err := dStore.Get(dFile)
	if err != nil {
		if reader != nil {
			_ = reader.Close()
		}
		return fmt.Errorf("failed to retrieve DFile %s from DStore: %w", dFile, err)
	}
	actualContent, err := io.ReadAll(reader)
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("failed to read DFile %s: %w", dFile, err)
	}
	if err := reader.Close(); err != nil {
		return fmt.Errorf("failed to close DFile %s: %w", dFile, err)
	}
	if !reflect.DeepEqual(expectedContent, actualContent) {
		return fmt.Errorf("unexpected content DFile for %s: %v != %v", dFile, expectedContent, actualContent)
	}
	return nil
}
