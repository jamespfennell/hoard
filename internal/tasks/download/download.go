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

// RunPeriodically runs the download task periodically, with the period specified
// in the feed configuration.
func RunPeriodically(session *tasks.Session) {
	feed := session.Feed()
	session.Log().Info("Starting periodic downloader")
	ticker := util.NewTicker(feed.Periodicity, 0)
	defer ticker.Stop()
	client := &http.Client{}
	var lastHash storage.Hash
	for {
		select {
		case <-ticker.C:
			dFile, err := downloadOnce(feed, session.LocalDStore(), lastHash, client, defaultTimeGetter)
			monitoring.RecordDownload(feed, err)
			if err != nil {
				session.Log().Errorf("Error downloading file: %s", err)
				continue
			}
			lastHash = dFile.Hash
		case <-session.Ctx().Done():
			session.Log().Info("Stopped periodic downloader")
			return
		}
	}
}

// RunOnce runs the download task once.
func RunOnce(session *tasks.Session) error {
	client := &http.Client{}
	_, err := downloadOnce(session.Feed(), session.LocalDStore(), "", client, defaultTimeGetter)
	return err
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
