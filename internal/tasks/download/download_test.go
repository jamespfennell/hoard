package download

import (
	"bytes"
	"errors"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"io"
	"net/http"
	"net/http/httptest"
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
	if err := testutil.DStoreHasDFile(d, expectedDFile, content1); err != nil {
		t.Errorf("Unexpected error: %s", err)
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

func TestDownloadOnce_Timeout(t *testing.T) {
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock
	}))
	defer server.Close()
	defer close(unblock)
	oldTimeout := downloadTimeout
	downloadTimeout = 100 * time.Millisecond
	defer func() { downloadTimeout = oldTimeout }()
	f := feed
	f.URL = server.URL

	done := make(chan error, 1)
	go func() {
		_, err := downloadOnce(&f, dstore.NewInMemoryDStore(), "", newHTTPClient(), returnTime1)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Errorf("Expected a timeout error")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Download did not time out")
	}
}

// closeTrackingBody is a response body that records whether it was closed.
type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

type closeTrackingHTTPClient struct {
	body   *closeTrackingBody
	status int
}

func (client closeTrackingHTTPClient) Do(*http.Request) (*http.Response, error) {
	return &http.Response{
		Body:       client.body,
		StatusCode: client.status,
	}, nil
}

func TestDownloadOnce_BadResponseCodeClosesBody(t *testing.T) {
	body := &closeTrackingBody{Reader: bytes.NewReader(content1)}
	client := closeTrackingHTTPClient{body: body, status: http.StatusInternalServerError}

	_, err := downloadOnce(&feed, dstore.NewInMemoryDStore(), "", client, returnTime1)

	if err == nil {
		t.Errorf("Expected an error for a non-200 response")
	}
	if !body.closed {
		t.Errorf("Response body was not closed")
	}
}
