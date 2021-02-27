package storage

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"testing"
	"time"
)

func TestDFile_StringRoundTrip(t *testing.T) {
	for i, d := range []DFile{
		{
			Prefix:  "a",
			Postfix: "b",
			Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
			Hash:    ExampleHash(),
		},
		{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
			Hash:    ExampleHash(),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			d2, ok := NewDFileFromString(d.String())

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
	for i, d := range []AFile{
		{
			Prefix: "a",
			Hour:   Hour(time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)),
			Hash:   ExampleHash(),
		},
		{
			Prefix: "",
			Hour:   Hour(time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)),
			Hash:   ExampleHash(),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			d2, ok := NewAFileFromString(d.String())

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
	expected := Hour(time.Date(2021, 2, 6, 22, 0, 0, 0, time.UTC))

	actual, ok := NewHourFromPersistencePrefix(p)

	if !ok {
		t.Fatal("Unexpected failure to convert prefix to hour", p)
	}
	if actual != expected {
		t.Error("Actual != expected; ", actual, expected)
	}
}
