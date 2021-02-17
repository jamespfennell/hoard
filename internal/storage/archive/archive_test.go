package archive

import (
	"github.com/jamespfennell/hoard/internal/storage"
	"reflect"
	"testing"
	"time"
)

var b1 = []byte{50, 51, 52}
var b2 = []byte{60, 61, 62}
var h = storage.Hour(time.Date(2000, 1, 2, 3, 0, 0, 0, time.UTC))
var d1 = storage.DFile{
	Prefix:  "a1",
	Postfix: "b1",
	Time:    time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC),
	Hash:    calculateHash(b1),
}
var d2 = storage.DFile{
	Prefix:  "a2",
	Postfix: "b2",
	Time:    time.Date(2000, 1, 2, 3, 5, 5, 0, time.UTC),
	Hash:    calculateHash(b2),
}
var d3 = storage.DFile{
	Prefix:  "a3",
	Postfix: "b3",
	Time:    time.Date(2000, 1, 2, 3, 6, 5, 0, time.UTC),
	Hash:    calculateHash(b2),
}

// TODO: round trip test source archives to test DFile reconstruction
//  from source archives
func TestArchive_RoundTrip(t *testing.T) {
	a := NewArchiveForWriting(h)
	errorOrFail(t, a.Store(d1, b1))
	errorOrFail(t, a.Store(d2, b2))
	errorOrFail(t, a.Store(d3, b2))

	l := a.Lock()
	bytes, err := l.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize the pack: %s", err)
	}
	a2, err := NewArchiveFromSerialization(bytes)

	if err != nil {
		t.Errorf("Unexpected error when deserializing pack: %s", err)
	}
	if !reflect.DeepEqual(l, a2) {
		t.Errorf("Serialization roundtrip fails: \n%+v != \n%+v", l, a2)
	}
}

func TestLockedArchive_ListInHour(t *testing.T) {
	a := NewArchiveForWriting(h)
	errorOrFail(t, a.Store(d1, b1))
	errorOrFail(t, a.Store(d2, b2))
	errorOrFail(t, a.Store(d3, b2))

	dFiles, err := a.Lock().ListInHour(h)

	if err != nil {
		t.Errorf("Unexpected error when listing files: %s", err)
	}
	if !reflect.DeepEqual([]storage.DFile{d1, d2, d3}, dFiles) {
		t.Errorf("Unexpected DFiles returned: %v != %v", []storage.DFile{d1, d2, d3}, dFiles)
	}
}

func TestLockedArchive_ListInDifferentHour(t *testing.T) {
	a := NewArchiveForWriting(h)
	errorOrFail(t, a.Store(d1, b1))
	errorOrFail(t, a.Store(d2, b2))
	errorOrFail(t, a.Store(d3, b2))

	oneHourBefore := storage.Hour(time.Time(h).Add(-1 * time.Hour))
	dFiles, err := a.Lock().ListInHour(oneHourBefore)

	if err != nil {
		t.Errorf("Unexpected error when listing files: %s", err)
	}
	if len(dFiles) != 0 {
		t.Errorf("Unexpected DFiles returned: %v != %v", nil, dFiles)
	}
}

func TestLockedArchive_ListNonEmptyHours1(t *testing.T) {
	a := NewArchiveForWriting(h)
	errorOrFail(t, a.Store(d1, b1))
	hours, err := a.Lock().ListNonEmptyHours()

	if err != nil {
		t.Errorf("Unexpected error when listing files: %s", err)
	}
	if len(hours) != 1 || hours[0] != h {
		t.Errorf("Unexpected hours returned: %v", hours)
	}
}

func TestLockedArchive_ListNonEmptyHours2(t *testing.T) {
	a := NewArchiveForWriting(h)
	hours, err := a.Lock().ListNonEmptyHours()

	if err != nil {
		t.Errorf("Unexpected error when listing files: %s", err)
	}
	if len(hours) != 0 {
		t.Errorf("Unexpected hours returned: %v", hours)
	}
}

func TestArchive_ReadAfterWriting(t *testing.T) {
	a := NewArchiveForWriting(h)
	errorOrFail(t, a.Store(d1, b1))
	errorOrFail(t, a.Store(d2, b2))
	errorOrFail(t, a.Store(d3, b2))
	l := a.Lock()

	d1Data, err1 := l.Get(d1)
	d2Data, err2 := l.Get(d2)
	d3Data, err3 := l.Get(d3)
	for i, err := range []error{err1, err2, err3} {
		if err != nil {
			t.Errorf("Unexpected error when getting file %d: %s", i, err)
		}
	}
	if !reflect.DeepEqual(d1Data, b1) {
		t.Errorf("%v != %v", d1Data, b1)
	}
	if !reflect.DeepEqual(d2Data, b2) {
		t.Errorf("%v != %v", d2Data, b2)
	}
	if !reflect.DeepEqual(d3Data, b2) {
		t.Errorf("%v != %v", d3Data, b2)
	}
}

func calculateHash(b []byte) storage.Hash {
	h := storage.CalculateHash(b)
	return h
}

func errorOrFail(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}
}