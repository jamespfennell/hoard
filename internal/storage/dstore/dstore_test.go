package dstore

import (
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestByteStorageBackedDStore_StoreGetDelete(t *testing.T) {
	b := persistence.NewInMemoryBytesStorage()
	d := NewByteStorageBackedDStore(b)

	dFile := storage.DFile{
		Hash:    storage.Hash("123"),
		Time:    time.Unix(200, int64(time.Millisecond)*5),
		Prefix:  "A",
		Postfix: "B",
	}
	content := []byte{60, 61, 62}

	err := d.Store(dFile, content)
	if err != nil {
		t.Fatalf("unexpected error when storing: %s", err)
	}

	retrievedContent, err := d.Get(dFile)
	if err != nil {
		t.Errorf("unexpected error when getting: %s", err)
	}
	if !reflect.DeepEqual(content, retrievedContent) {
		t.Errorf("stored content (%v) != retrieved content (%v)", content, retrievedContent)
	}

	err = d.Delete(dFile)
	if err != nil {
		t.Errorf("unexpected error when deleting: %s", err)
	}

	// Verify the file is actually deleted
	_, err = d.Get(dFile)
	if err == nil {
		t.Errorf("expected not found error when getting deleting file; got no error")
	}
}

func TestByteStorageBackedDStore_ListNonEmptyHours(t *testing.T) {
	b := persistence.NewInMemoryBytesStorage()
	d := NewByteStorageBackedDStore(b)

	time1 := time.Date(2000, 1, 2, 3, 4, 5, int(time.Millisecond)*5, time.UTC)
	hour1 := hour.Date(2000, 1, 2, 3)
	time2 := time.Date(2000, 2, 2, 3, 4, 5, int(time.Millisecond)*5, time.UTC)
	hour2 := hour.Date(2000, 2, 2, 3)
	if err := d.Store(storage.DFile{
		Hash:    "123",
		Time:    time1,
		Prefix:  "A",
		Postfix: "B",
	}, nil); err != nil {
		t.Fatalf("unexpected error when storing: %s", err)
	}
	if err := d.Store(storage.DFile{
		Hash:    "456",
		Time:    time2,
		Prefix:  "A",
		Postfix: "B",
	}, nil); err != nil {
		t.Fatalf("unexpected error when storing: %s", err)
	}

	actualHours, err := d.ListNonEmptyHours()
	if err != nil {
		t.Errorf("unexpected error when listing: %s", err)
	}
	correct := len(actualHours) == 2 && ((actualHours[0] == hour1 && actualHours[1] == hour2) ||
		(actualHours[1] == hour1 && actualHours[0] == hour2))
	if !correct {
		t.Errorf("unexpected hours: %v != [%v, %v]", actualHours, hour1, hour2)
	}
}

func TestByteStorageBackedDStore_ListInHour(t *testing.T) {
	b := persistence.NewInMemoryBytesStorage()
	d := NewByteStorageBackedDStore(b)

	time1 := time.Date(2000, 1, 2, 3, 4, 5, int(time.Millisecond)*5, time.UTC)
	hour1 := hour.Date(2000, 1, 2, 3)
	time2 := time.Date(2000, 1, 2, 4, 4, 5, int(time.Millisecond)*5, time.UTC)

	dFile1 := storage.DFile{
		Hash:    storage.ExampleHash(),
		Time:    time1,
		Prefix:  "A",
		Postfix: "B",
	}
	dFile2 := storage.DFile{
		Hash:    storage.ExampleHash(),
		Time:    time2,
		Prefix:  "A",
		Postfix: "B",
	}
	if err := d.Store(dFile1, nil); err != nil {
		t.Fatalf("unexpected error when storing: %s", err)
	}
	if err := d.Store(dFile2, nil); err != nil {
		t.Fatalf("unexpected error when storing: %s", err)
	}

	resultDFiles, err := d.ListInHour(hour1)
	if err != nil {
		t.Errorf("unexpected error when listing: %s", err)
	}
	if len(resultDFiles) != 1 || resultDFiles[0] != dFile1 {
		t.Errorf("unexpected DFiles: %v != [%v]", resultDFiles, dFile1)
	}
}

func TestByteStorageBackedDStore_ImplementationDetails(t *testing.T) {
	// This is a test of the mapping from DFiles to persistence keys.
	// It seems like this is an implementation test and hence not good, but
	// in fact because the persistence key structure maps onto the directory structure
	// of stored files, this "implementation" is a part of the Hoard public API.
	b := persistence.NewInMemoryBytesStorage()
	d := NewByteStorageBackedDStore(b)

	time1 := time.Date(2000, 1, 2, 3, 4, 5, int(time.Millisecond)*5, time.UTC)
	dFile1 := storage.DFile{
		Hash:    storage.ExampleHash(),
		Time:    time1,
		Prefix:  "A",
		Postfix: "B",
	}
	key1 := persistence.Key{
		Prefix: []string{"2000", "01", "02", "03"},
		Name:   "A20000102T030405.005Z_aaaaaaaaaaaaB",
	}
	content := []byte{70, 71, 72}

	if err := d.Store(dFile1, content); err != nil {
		t.Fatal("unexpected error when storing dFile", err)
	}

	reader, err := b.Get(key1)
	if err != nil {
		t.Errorf("unexpected error when retrieving key: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected error when reading: %v", err)
	}
	if !reflect.DeepEqual(data, content) {
		t.Errorf("DFile content (%v) != key content (%v)", content, data)
	}
}
