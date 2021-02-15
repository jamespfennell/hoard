package merge

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/testutil"
	"reflect"
	"testing"
	"time"
)

var b1 = []byte{50, 51, 52}
var b2 = []byte{60, 61, 62}
var b3 = []byte{70, 71, 72}
var h = storage.Hour(time.Date(2000, 1, 2, 3, 0, 0, 0, time.UTC))
var d1 = storage.DFile{
	Prefix:  "",
	Postfix: "",
	Time:    time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC),
	Hash:    storage.CalculateHash(b1),
}
var d2 = storage.DFile{
	Prefix:  "",
	Postfix: "",
	Time:    time.Date(2000, 1, 2, 3, 5, 5, 0, time.UTC),
	Hash:    storage.CalculateHash(b2),
}
var d3 = storage.DFile{
	Prefix:  "",
	Postfix: "",
	Time:    time.Date(2000, 1, 2, 3, 6, 5, 0, time.UTC),
	Hash:    storage.CalculateHash(b3),
}
var feed = &config.Feed{}

func TestOnce(t *testing.T) {
	a := astore.NewInMemoryAStore()
	createArchive(t, a, d1, b1, d2, b2)
	createArchive(t, a, d2, b2, d3, b3)

	testutil.ErrorOrFail(t, Once(feed, a))

	aFiles, err := a.ListInHour(h)
	if err != nil {
		t.Errorf("Unexpected error in ListInHour: %s\n", err)
	}
	if len(aFiles) != 1 {
		t.Errorf("Unexpected number of AFiles: 3 != %d\n", len(aFiles))
	}

	aFile := aFiles[0]
	content, err := a.Get(aFile)
	if err != nil {
		t.Errorf("Unexpected error when getting AFile: %s\n", err)
	}
	ar, err := archive.NewArchiveFromSerialization(content)
	if err != nil {
		t.Errorf("Unexpected error deserializing archive: %s\n", err)
	}

	dFiles, err := ar.ListInHour(h)
	if err != nil {
		t.Errorf("Unexpected error listing DFiles in archive: %s\n", err)
	}
	if !reflect.DeepEqual(dFiles, []storage.DFile{d1, d2, d3}) {
		t.Errorf("%v != %v", dFiles, []storage.DFile{d1, d2, d3})
	}

	for _, dFileAndContent := range []struct {
		dFile   storage.DFile
		content []byte
	}{
		{d1, b1},
		{d2, b2},
		{d3, b3},
	} {
		bRecovered, err := ar.Get(dFileAndContent.dFile)
		if err != nil {
			t.Errorf("Unexpected error when retrieving %s: %s", dFileAndContent.dFile, err)
		}
		if !reflect.DeepEqual(dFileAndContent.content, bRecovered) {
			t.Errorf("Unexpected content for %s: %v != %v", dFileAndContent.dFile, dFileAndContent.content, bRecovered)
		}
	}
}

func createArchive(t *testing.T, a storage.AStore, d1 storage.DFile, b1 []byte, d2 storage.DFile, b2 []byte) {
	ar1 := archive.NewArchiveForWriting(h)
	testutil.ErrorOrFail(t, ar1.Store(d1, b1))
	testutil.ErrorOrFail(t, ar1.Store(d2, b2))
	l1 := ar1.Lock()
	b, err := l1.Serialize()
	testutil.ErrorOrFail(t, err)
	testutil.ErrorOrFail(t, a.Store(storage.AFile{
		Prefix: "",
		Hash:   l1.Hash(),
		Time:   h,
	}, b))

}
