package download

import (
	"bytes"
	"errors"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"io"
	"net/http"
	"testing"
	"time"
)

const feedID1 = "feed"
const prefix1 = "feed_"
const postfix1 = ".html"
const url1 = "http://www.example.com"

var feed = config.Feed{
	ID:      feedID1,
	Postfix: postfix1,
	URL:     url1,
}

var content1 = []byte{75, 76, 77}
var content2 = []byte{85, 86, 87}

const hash1 = "hcievffr4p3i"
const hash2 = "yxeb7idlhuev"

var time1 = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)

func returnTime1() time.Time {
	return time1
}

type httpClientForTesting struct {
	body   []byte
	status int
}

func (client httpClientForTesting) Do(*http.Request) (*http.Response, error) {
	if client.body == nil && client.status == 0 {
		return nil, errors.New("simulated error")
	}
	if client.status == 0 {
		client.status = http.StatusOK
	}
	return &http.Response{
		Body:       io.NopCloser(bytes.NewReader(client.body)),
		StatusCode: client.status,
	}, nil
}

func TestDownloadOnce(t *testing.T) {
	d := dstore.NewInMemoryDStore()
	client := httpClientForTesting{
		body: content1,
	}
	expectedDFile := storage.DFile{
		Prefix:  prefix1,
		Postfix: postfix1,
		Hash:    hash1,
		Time:    time1,
	}

	actualDFile, err := downloadOnce(&feed, d, "", client, returnTime1)

	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if expectedDFile != *actualDFile {
		t.Errorf("Unexpected DFile %v; expected %v", *actualDFile, expectedDFile)
	}
	if d.Count() != 1 {
		t.Errorf("Unexpected number of files in the persistence: 1!=%d", d.Count())
	}
	actualContent, err := d.Get(expectedDFile)
	if err != nil {
		t.Errorf("Could not find DFile %s", expectedDFile)
		return
	}
	if !bytesEqual(actualContent, content1) {
		t.Errorf("Content not the same. Actual: %v; expected: %v", actualContent, content1)
	}
}

func TestDownloadOnce_ErrorInExecuting(t *testing.T) {
	d := dstore.NewInMemoryDStore()
	client := httpClientForTesting{}

	_, err := downloadOnce(&feed, d, "", client, returnTime1)

	if err == nil {
		t.Errorf("Expected error; recieved none")
	}
}

func TestDownloadOnce_BadResponseCode(t *testing.T) {
	d := dstore.NewInMemoryDStore()
	client := httpClientForTesting{
		status: http.StatusBadGateway,
	}

	_, err := downloadOnce(&feed, d, "", client, returnTime1)

	if err == nil {
		t.Errorf("Expected HTTP bad gateway error; recieved none")
	}
}

func TestDownloadOnce_SkipRepeatedHash(t *testing.T) {
	d := dstore.NewInMemoryDStore()
	client := httpClientForTesting{
		body: content1,
	}

	_, err := downloadOnce(&feed, d, hash1, client, returnTime1)

	if err != nil {
		t.Errorf("Unexpected error")
	}
	if d.Count() != 0 {
		t.Errorf("Unexpected DFile written to the DStore")
	}
}

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
