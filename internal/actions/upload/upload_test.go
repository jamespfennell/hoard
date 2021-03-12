package upload

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"reflect"
	"testing"
	"time"
)

var b1 = []byte{50, 51, 52}
var b2 = []byte{60, 61, 62}
var b3 = []byte{70, 71, 72}
var h = hour.Date(2000, 1, 2, 3)
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
	localAStore := astore.NewInMemoryAStore()
	remoteAStore := astore.NewInMemoryAStore()
	createArchive(t, localAStore, d1, b1, d2, b2)
	createArchive(t, remoteAStore, d2, b2, d3, b3)

	err := Once(feed, localAStore, remoteAStore)
	testutil.ErrorOrFail(t, err)

	localAFiles, err := storage.ListAFilesInHour(localAStore, h)
	if err != nil {
		t.Errorf("Unexpected error in ListInHour: %s\n", err)
	}
	if len(localAFiles) != 0 {
		t.Errorf("Unexpected number of AFiles: 0 != %d\n", len(localAFiles))
	}

	remoteAFiles, err := storage.ListAFilesInHour(remoteAStore, h)
	if err != nil {
		t.Errorf("Unexpected error in ListInHour: %s\n", err)
	}
	if len(remoteAFiles) != 1 {
		t.Errorf("Unexpected number of AFiles: 1 != %d\n", len(remoteAFiles))
	}

	aFile := remoteAFiles[0]
	content, err := remoteAStore.Get(aFile)
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
		if err := testutil.DStoreHasDFile(ar, dFileAndContent.dFile, dFileAndContent.content); err != nil {
			t.Errorf("Error: %s", err)
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
		Hour:   h,
	}, b))

}
