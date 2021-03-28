package upload

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"testing"
)

var h = hour.Date(2000, 1, 2, 3)
var feed = &config.Feed{}

func TestOnce(t *testing.T) {
	localAStore := astore.NewInMemoryAStore()
	remoteAStore := astore.NewInMemoryAStore()
	testutil.CreateArchiveFromData(t, localAStore, testutil.Data[0], testutil.Data[1])
	testutil.CreateArchiveFromData(t, remoteAStore, testutil.Data[1], testutil.Data[3])
	// createArchive(t, localAStore, d1, b1, d2, b2)
	// createArchive(t, remoteAStore, d2, b2, d3, b3)

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
	dStore := dstore.NewInMemoryDStore()
	err = archive.Unpack(content, dStore)
	if err != nil {
		t.Errorf("Unexpected error deserializing archive: %s\n", err)
	}

	testutil.ExpectDStoreHasExactlyDFiles(t, dStore, testutil.Data[0], testutil.Data[1], testutil.Data[3])
}
