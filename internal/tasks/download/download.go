// Package download contains the download task.
//
// This task downloads data feeds of interest and stores them on local disk.
package download

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/util"
)

type download struct {
	lastHash storage.Hash
	client   *http.Client
}

func New() tasks.Task {
	return &download{
		client: &http.Client{},
	}
}

func (d *download) PeriodicTicker(session *tasks.Session) *util.Ticker {
	t := util.NewTicker(session.Feed().Periodicity, 0)
	return &t
}

func (d *download) Run(session *tasks.Session) error {
	dFile, err := downloadOnce(session.Feed(), session.LocalDStore(), d.lastHash, d.client, defaultTimeGetter)
	d.lastHash = dFile.Hash
	return err
}

func (d *download) Name() string {
	return "download"
}

type timeGetter func() time.Time

func defaultTimeGetter() time.Time {
	return time.Now().UTC()
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func downloadOnce(feed *config.Feed, dstore storage.DStore, lastHash storage.Hash, client httpClient, now timeGetter) (*storage.DFile, error) {
	req, err := http.NewRequest("GET", feed.URL, nil)
	if err != nil {
		return nil, err
	}
	for key, value := range feed.Headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status recieved: %d / %s", resp.StatusCode, resp.Status)
	}
	// We read the whole content into memory so that we can calculate the hash
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	hash := storage.CalculateHash(content)
	dFile := storage.DFile{
		Prefix:  feed.Prefix(),
		Postfix: feed.Postfix,
		Time:    now(),
		Hash:    hash,
	}
	if hash == lastHash {
		return &dFile, nil
	}
	err = dstore.Store(dFile, bytes.NewReader(content))
	if err != nil {
		return nil, err
	}
	monitoring.RecordSavedDownload(feed, len(content))
	return &dFile, nil
}
