package storage_test

import (
	"bytes"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util/testutil"
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

func TestCopy(t *testing.T) {
	data1 := testutil.Data[0]
	data2 := testutil.Data[1]

	source := dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, source.Store(data1.DFile, bytes.NewReader(data1.Content)))
	testutil.ErrorOrFail(t, source.Store(data2.DFile, bytes.NewReader(data2.Content)))

	target := dstore.NewInMemoryDStore()

	result, err := storage.Copy(source, target, data1.Hour)
	if err != nil {
		t.Errorf("unexpected copy error: %s", err)
	}
	if len(result.CopyErrors) != 0 {
		t.Errorf("unexpected copy errors: %s", result.CopyErrors)
	}

	expectedDFilesCopied := map[storage.DFile]bool{
		data1.DFile: true,
		data2.DFile: true,
	}
	if len(result.DFilesCopied) != 2 {
		t.Errorf("Expected 2 files to be copied, found %v", result.DFilesCopied)
	}
	for _, dFileCopied := range result.DFilesCopied {
		if !expectedDFilesCopied[dFileCopied] {
			t.Errorf("DFile %s not copied", dFileCopied)
		}
	}
	if result.BytesCopied != len(data1.Content)+len(data2.Content) {
		t.Errorf("unexpected number of bytes copied: %d", result.BytesCopied)
	}

	if target.Count() != 2 {
		t.Errorf("Expected 2 files in storage, found %d", target.Count())
	}
	if _, err := target.Get(data1.DFile); err != nil {
		t.Errorf("File %s not copied!", data1.DFile)
	}
	if _, err := target.Get(data2.DFile); err != nil {
		t.Errorf("File %s not copied!", data2.DFile)
	}
}
