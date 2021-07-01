package storage_test

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/compression"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"testing"
	"time"
)

func TestDFile_StringRoundTrip(t *testing.T) {
	for i, d := range []storage.DFile{
		{
			Prefix:  "a",
			Postfix: "b",
			Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
			Hash:    storage.ExampleHash(),
		},
		{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
			Hash:    storage.ExampleHash(),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			d2, ok := storage.NewDFileFromString(d.String())

			if !ok {
				t.Errorf("Expected %s could be converted to a DFile", d.String())
			}
			if d != d2 {
				t.Errorf("\n%v!= \n%v", d, d2)
			}
		})
	}
}

func TestAFile_LegacyFileName(t *testing.T) {
	// Before support for multiple compression formats, AFiles had this format. We still need to support them.
	fileName := "a20200102T03Z_aaaaaaaaaaaa.tar.gz"
	expectedAFile := storage.AFile{
		Prefix:      "a",
		Hour:        hour.Date(2020, 1, 2, 3),
		Hash:        storage.ExampleHash(),
		Compression: compression.NewSpecWithLevel(compression.Gzip, 6),
	}
	actualAFile, ok := storage.NewAFileFromString(fileName)
	if !ok {
		t.Errorf("Expected %s could be converted to a AFile", fileName)
	}
	if expectedAFile != actualAFile {
		t.Errorf("\n%v != \n%v", expectedAFile, actualAFile)
	}
}

func TestAFile_StringRoundTrip(t *testing.T) {
	for i, d := range []storage.AFile{
		{
			Prefix:      "a",
			Hour:        hour.Date(2020, 1, 2, 3),
			Hash:        storage.ExampleHash(),
			Compression: compression.NewSpecWithLevel(compression.Gzip, 6),
		},
		{
			Prefix:      "",
			Hour:        hour.Date(2020, 1, 2, 3),
			Hash:        storage.ExampleHash(),
			Compression: compression.NewSpecWithLevel(compression.Gzip, 6),
		},
		{
			Prefix:      "",
			Hour:        hour.Date(2020, 1, 2, 3),
			Hash:        storage.ExampleHash(),
			Compression: compression.NewSpecWithLevel(compression.Xz, 6),
		},
		{
			Prefix:      "",
			Hour:        hour.Date(2020, 1, 2, 3),
			Hash:        storage.ExampleHash(),
			Compression: compression.NewSpecWithLevel(compression.Gzip, 2),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			d2, ok := storage.NewAFileFromString(d.String())

			if !ok {
				t.Errorf("Expected %s could be converted to a AFile", d.String())
			}
			if d != d2 {
				t.Errorf("\n%v != \n%v", d, d2)
			}
		})
	}
}

func TestAFile_StringRoundTripWithDefaultCompression(t *testing.T) {
	input := storage.AFile{
		Prefix:      "a",
		Hour:        hour.Date(2020, 1, 2, 3),
		Hash:        storage.ExampleHash(),
		Compression: compression.Spec{},
	}
	expectedOutput := storage.AFile{
		Prefix:      "a",
		Hour:        hour.Date(2020, 1, 2, 3),
		Hash:        storage.ExampleHash(),
		Compression: compression.NewSpecWithLevel(compression.Gzip, 6),
	}
	actualOutput, ok := storage.NewAFileFromString(input.String())
	if !ok {
		t.Errorf("Expected %s could be converted to a AFile", input.String())
	}
	if expectedOutput != actualOutput {
		t.Errorf("\n%v != \n%v", expectedOutput, actualOutput)
	}
}

func TestPersistencePrefixToHour(t *testing.T) {
	p := persistence.Prefix{"2021", "02", "06", "22"}
	expected := hour.Date(2021, 2, 6, 22)

	actual, ok := hour.NewHourFromPersistencePrefix(p)

	if !ok {
		t.Fatal("Unexpected failure to convert prefix to hour", p)
	}
	if actual != expected {
		t.Error("Actual != expected; ", actual, expected)
	}
}
