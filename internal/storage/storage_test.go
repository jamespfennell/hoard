package storage_test

import (
	"fmt"
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
				t.Errorf("%v != %v", d, d2)
			}
		})
	}
}

func TestAFile_StringRoundTrip(t *testing.T) {
	for i, d := range []storage.AFile{
		{
			Prefix: "a",
			Hour:   hour.Date(2020, 1, 2, 3),
			Hash:   storage.ExampleHash(),
		},
		{
			Prefix: "",
			Hour:   hour.Date(2020, 1, 2, 3),
			Hash:   storage.ExampleHash(),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			d2, ok := storage.NewAFileFromString(d.String())

			if !ok {
				t.Errorf("Expected %s could be converted to a DFile", d.String())
			}
			if d != d2 {
				t.Errorf("%v != %v", d, d2)
			}
		})
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
