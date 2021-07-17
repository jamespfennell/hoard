package testutil

import (
	"bytes"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"io"
	"reflect"
	"testing"
	"time"
)

type DFileData struct {
	Content []byte
	DFile   storage.DFile
	Hour    hour.Hour
}

var Data = []DFileData{
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

// TODO: rename
// TODO: accept a string describing the error
func ErrorOrFail(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Unexpected error '%s'", err)
	}
}

func ExpectDStoreHasExactlyDFiles(t *testing.T, dStore storage.ReadableDStore, dFiles ...DFileData) {
	type counter interface {
		Count() int
	}
	if countableDStore, ok := dStore.(counter); ok {
		if countableDStore.Count() != len(dFiles) {
			t.Errorf("Unexpected number of DFiles %d; expected %d", countableDStore.Count(), len(dFiles))
		}
	}
	for _, dFile := range dFiles {
		if err := DStoreHasDFile(dStore, dFile.DFile, dFile.Content); err != nil {
			t.Errorf("DFile not in DStore: %s", err)
		}
	}
}

// TODO: Expect and use DFileData
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

func CreateArchiveFromData(t *testing.T, f *config.Feed, aStore storage.AStore, dFileData ...DFileData) storage.AFile {
	dStore := dstore.NewInMemoryDStore()
	var dFiles []storage.DFile
	for _, dFile := range dFileData {
		ErrorOrFail(t, dStore.Store(dFile.DFile, bytes.NewReader(dFile.Content)))
		dFiles = append(dFiles, dFile.DFile)
	}
	aFile, _, err := archive.CreateFromDFiles(f, dFiles, dStore, aStore)
	ErrorOrFail(t, err)
	return aFile
}

func CompareBytes(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}
	for i := range b1 {
		if b1[i] != b2[i] {
			return false
		}
	}
	return true
}
