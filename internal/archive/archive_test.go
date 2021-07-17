package archive_test

import (
	"bytes"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"testing"
)

func TestCreateFromDFiles(t *testing.T) {
	for i, compression := range []config.Compression{
		config.NewSpecWithLevel(config.Gzip, 5),
		config.NewSpecWithLevel(config.Xz, 5),
	} {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {

			data1 := testutil.Data[0]
			dStore := dstore.NewInMemoryDStore()
			testutil.ErrorOrFail(t, dStore.Store(data1.DFile, bytes.NewReader(data1.Content)))
			aStore := astore.NewInMemoryAStore()

			aFile, incorporatedDFiles, err := archive.CreateFromDFiles(
				&config.Feed{Compression: compression}, []storage.DFile{data1.DFile}, dStore, aStore)
			testutil.ErrorOrFail(t, err)

			if aFile.Hour != data1.Hour {
				t.Errorf("Archive has unexpected hour %s; expected %s", aFile.Hour, data1.Hour)
			}
			if len(incorporatedDFiles) != 1 || incorporatedDFiles[0] != data1.DFile {
				t.Errorf("Unexpected DFiles incorporated: %s; expected %s", incorporatedDFiles, data1.DFile)
			}

			dStore = dstore.NewInMemoryDStore()
			testutil.ErrorOrFail(t, archive.Unpack(aFile, aStore, dStore))

			testutil.ExpectDStoreHasExactlyDFiles(t, dStore, data1)
		})
	}
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
	aStore := astore.NewInMemoryAStore()

	aFile, incorporatedDFiles, err := archive.CreateFromDFiles(
		&config.Feed{}, []storage.DFile{data1.DFile, data2.DFile, data3.DFile}, dStore, aStore)
	testutil.ErrorOrFail(t, err)
	if len(incorporatedDFiles) != 3 {
		t.Errorf("Unexpected DFiles incorporated: %s; expected 3 dFiles", incorporatedDFiles)
	}
	dStore = dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, archive.Unpack(aFile, aStore, dStore))

	testutil.ExpectDStoreHasExactlyDFiles(t, dStore, data1, data2)
}

// TODO Test case of CreateFromDFiles 3 DFiles, two duplicates apart

func TestCreateFromAFiles(t *testing.T) {
	feed := &config.Feed{}
	sourceAStore := astore.NewInMemoryAStore()
	targetAStore := astore.NewInMemoryAStore()
	data1 := testutil.Data[0]
	data2 := testutil.Data[1]
	aFile1 := testutil.CreateArchiveFromData(t, feed, sourceAStore, data1)
	aFile2 := testutil.CreateArchiveFromData(t, feed, sourceAStore, data2)

	newAFile, _, err := archive.CreateFromAFiles(feed, []storage.AFile{aFile1, aFile2},
		sourceAStore, targetAStore, dstore.NewInMemoryDStore())
	testutil.ErrorOrFail(t, err)

	dStore := dstore.NewInMemoryDStore()
	testutil.ErrorOrFail(t, archive.Unpack(newAFile, targetAStore, dStore))

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

func TestRecompress(t *testing.T) {
	for _, testCase := range []struct {
		oldCompression config.Compression
		newCompression config.Compression
	}{
		{
			config.NewSpecWithLevel(config.Gzip, 6),
			config.NewSpecWithLevel(config.Xz, 9),
		},
		{
			config.NewSpecWithLevel(config.Xz, 9),
			config.NewSpecWithLevel(config.Gzip, 6),
		},
	} {
		oldFeed := &config.Feed{Compression: testCase.oldCompression}
		newFeed := &config.Feed{Compression: testCase.newCompression}

		sourceAStore := astore.NewInMemoryAStore()
		targetAStore := astore.NewInMemoryAStore()

		data := []testutil.DFileData{testutil.Data[0], testutil.Data[1], testutil.Data[3]}
		oldAFile := testutil.CreateArchiveFromData(t, oldFeed, sourceAStore, data...)

		newAFile, err := archive.Recompress(newFeed, oldAFile, sourceAStore, targetAStore)
		testutil.ErrorOrFail(t, err)

		dStore := dstore.NewInMemoryDStore()
		testutil.ErrorOrFail(t, archive.Unpack(newAFile, targetAStore, dStore))
		testutil.ExpectDStoreHasExactlyDFiles(t, dStore, data...)
	}
}
