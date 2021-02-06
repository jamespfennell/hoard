package download

import (
	"errors"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"testing"
	"time"
)

const feedID1 = "feed"
const prefix1 = "feed_"
const postfix1 = ".html"

var content1 = []byte{75, 76, 77}
const hash1 = "hcievffr4p3i"

var content2 = []byte{85, 86, 87}
const hash2 = "yxeb7idlhuev"

var time1 = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)

const url1 = "http://www.example.com"
const url2 = "http://www.example2.com"

func createHttpGetter(url string, content []byte) httpGetter {
	return func(requestUrl string) ([]byte, error) {
		if requestUrl != url {
			return nil, errors.New("")
		}
		return content, nil
	}
}

func TestDownloadFeed(t *testing.T) {
	feed := config.Feed{
		ID:      feedID1,
		Prefix:  prefix1,
		Postfix: postfix1,
		URL:     url1,
	}
	dstore := storage.NewInMemoryDStore()
	_, err := downloadFeed(&feed, dstore, "", createHttpGetter(url1, content1),
		func() time.Time { return time1 },
	)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if dstore.Count() != 1 {
		t.Errorf("Unexpected number of files in the persistence: 1!=%d", dstore.Count())
	}

	expectedDFile := storage.DFile{
		Prefix:  prefix1,
		Postfix: postfix1,
		Hash:    hash1,
		Time:    time1,
	}
	actualContent, ok := dstore.Get(expectedDFile)
	if !ok {
		t.Errorf("Could not find DFile %s", expectedDFile)
		return
	}
	if !bytesEqual(actualContent, content1) {
		t.Errorf("Content not the same. Actual: %v; expected: %v", actualContent, content1)
	}
}

// TODO Test failed to download 404
// TODO Test multiple downloads
// TODO Test skipping when the hash is the same
// TODO test not skipping when the hash is the same but the hour is different

func bytesEqual(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}
	for i := range b1 {
		if b1[i] != b2[i] {
			return false
		}
	}
	return true
}
