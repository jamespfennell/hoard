package upload

import (
	"testing"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/util/testutil"
)

var h = hour.Date(2000, 1, 2, 3)
var feed = &config.Feed{}

func TestOnce(t *testing.T) {
	session := tasks.NewInMemorySession(feed)
	localAStore := session.LocalAStore()
	remoteAStore := session.RemoteAStore()
	testutil.CreateArchiveFromData(t, feed, localAStore, testutil.Data[0], testutil.Data[1])
	testutil.CreateArchiveFromData(t, feed, remoteAStore, testutil.Data[1], testutil.Data[3])

	err := RunOnce(session)
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
	if len(remoteAFiles) != 2 {
		t.Errorf("Unexpected number of AFiles: 1 != %d\n", len(remoteAFiles))
	}

	dStore := dstore.NewInMemoryDStore()
	for _, aFile := range remoteAFiles {
		err = archive.Unpack(aFile, remoteAStore, dStore)
		if err != nil {
			t.Errorf("Unexpected error deserializing archive: %s\n", err)
		}
	}

	testutil.ExpectDStoreHasExactlyDFiles(t, dStore, testutil.Data[0], testutil.Data[1], testutil.Data[3])
}
