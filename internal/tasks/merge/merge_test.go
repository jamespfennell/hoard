package merge

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/util/testutil"
)

var h = testutil.Data[0].Hour
var feed = &config.Feed{}

func TestOnce(t *testing.T) {
	a1 := astore.NewInMemoryAStore()
	testutil.CreateArchiveFromData(t, feed, a1, testutil.Data[0], testutil.Data[1])
	testutil.CreateArchiveFromData(t, feed, a1, testutil.Data[1], testutil.Data[3])

	// This is the case when the resulting merge is already in the AStore
	a2 := astore.NewInMemoryAStore()
	testutil.CreateArchiveFromData(t, feed, a2, testutil.Data[0], testutil.Data[1], testutil.Data[3])
	testutil.CreateArchiveFromData(t, feed, a2, testutil.Data[1], testutil.Data[3])

	aStore3 := astore.NewPersistedAStore(persistence.NewInMemoryPersistedStorage(), slog.Default())
	testutil.CreateArchiveFromData(t, feed, aStore3, testutil.Data[0], testutil.Data[1])
	testutil.CreateArchiveFromData(t, feed, aStore3, testutil.Data[0], testutil.Data[1], testutil.Data[3])

	for i, a := range []storage.AStore{a1, a2, aStore3} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			session := tasks.NewInMemorySession(feed)
			_, err := RunOnce(session, a)
			testutil.ErrorOrFail(t, err)

			aFiles, err := storage.ListAFilesInHour(a, h)
			if err != nil {
				t.Errorf("Unexpected error in ListInHour: %s\n", err)
			}
			if len(aFiles) == 0 {
				t.Fatalf("Unexpected number of AFiles: 0 != %d\n", len(aFiles))
			}
			if len(aFiles) != 1 {
				t.Errorf("Unexpected number of AFiles: 1 != %d\n", len(aFiles))
			}

			aFile := aFiles[0]
			dStore := dstore.NewInMemoryDStore()
			err = archive.Unpack(aFile, a, dStore)
			if err != nil {
				t.Errorf("Unexpected error when getting AFile: %s\n", err)
			}

			testutil.ExpectDStoreHasExactlyDFiles(t, dStore,
				testutil.Data[0], testutil.Data[1], testutil.Data[3])
		})
	}
}
