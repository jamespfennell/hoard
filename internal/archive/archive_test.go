package archive_test

import (
	"bytes"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"testing"
)

func TestCreateFromDFiles(t *testing.T) {
	data1 := testutil.Data[0]
	dStore := dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, dStore.Store(data1.DFile, bytes.NewReader(data1.Content)))

	a, err := archive.CreateFromDFiles(&config.Feed{}, []storage.DFile{data1.DFile}, dStore)
	testutil.ErrorOrFail(t, err)

	if a.AFile().Hour != data1.Hour {
		t.Errorf("Archive has unexpected hour %s; expected %s", a.AFile().Hour, data1.Hour)
	}
	if a.IncorporatedAFiles != nil {
		t.Errorf("Unexpected AFiles incorporated: %s; expected none", a.IncorporatedAFiles)
	}
	if len(a.IncorporatedDFiles) != 1 || a.IncorporatedDFiles[0] != data1.DFile {
		t.Errorf("Unexpected DFiles incorporated: %s; expected %s", a.IncorporatedDFiles, data1.DFile)
	}

	dStore = dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, archive.Unpack(a.Reader(), dStore))
	testutil.ErrorOrFail(t, a.Close())

	testutil.ExpectDStoreHasExactlyDFiles(t, dStore, data1)
}

func TestCreateFromDFiles_DuplicatesFiltered(t *testing.T) {
	data1 := testutil.Data[0]
	data2 := testutil.Data[1]
	data3 := testutil.Data[2]

	dStore := dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, dStore.Store(data1.DFile, bytes.NewReader(data1.Content)))
	testutil.ErrorOrFail(t, dStore.Store(data2.DFile, bytes.NewReader(data2.Content)))
	testutil.ErrorOrFail(t, dStore.Store(data3.DFile, bytes.NewReader(data3.Content)))
	if dStore.Count() != 3 {
		t.Errorf("Failed to store all 3 DFiles")
	}

	a, err := archive.CreateFromDFiles(&config.Feed{}, []storage.DFile{data1.DFile, data2.DFile, data3.DFile}, dStore)
	testutil.ErrorOrFail(t, err)
	if len(a.IncorporatedDFiles) != 3 {
		t.Errorf("Unexpected DFiles incorporated: %s; expected 3 dFiles", a.IncorporatedDFiles)
	}
	dStore = dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, archive.Unpack(a.Reader(), dStore))
	testutil.ErrorOrFail(t, a.Close())

	testutil.ExpectDStoreHasExactlyDFiles(t, dStore, data1, data2)
}

// TODO Test case of CreateFromDFiles 3 DFiles, two duplicates apart

func TestCreateFromAFiles(t *testing.T) {
	aStore := astore.NewInMemoryAStore()
	data1 := testutil.Data[0]
	data2 := testutil.Data[1]
	aFile1 := testutil.CreateArchiveFromData(t, aStore, data1)
	aFile2 := testutil.CreateArchiveFromData(t, aStore, data2)

	a, err := archive.CreateFromAFiles(&config.Feed{}, []storage.AFile{aFile1, aFile2}, aStore, dstore.NewInMemoryDStore())
	testutil.ErrorOrFail(t, err)
	dStore := dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, archive.Unpack(a.Reader(), dStore))
	testutil.ErrorOrFail(t, a.Close())

	testutil.ExpectDStoreHasExactlyDFiles(t, dStore, data1, data2)
}

// TODO Merge two archives together, (A A) and (B) and ensure 3 files (ABA) are outputted
//  ^ this test is basically why we have a manifest
// TODO Case when two archives contain the identical DFile
// TODO fail to read one DFile from the archive, and then that archive is not marked as incorporated
// Merge three archives together?

// Corrupted AFile: missing DFile
// Corrupted AFile: DFile not referenced in manifest
// Corrupted AFile: DFile reference in manifest missing
// Corrupted AFile: manifest can't be read
