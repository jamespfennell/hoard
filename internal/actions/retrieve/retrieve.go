// Package retrieve contains the retrieve action.
//
// This action retrieves data from remote storage and places them in prescribed
// directories locally.
package retrieve

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"sort"
	"sync"
)

type feedStatus struct {
	searched      bool
	numArchives   int
	numDownloaded int
	done          bool
}

func (fs *feedStatus) String() string {
	if fs.done {
		return "done"
	}
	if !fs.searched {
		return "searching remote storage"
	}
	return fmt.Sprintf(
		"downloaded %d/%d archives",
		fs.numDownloaded,
		fs.numArchives,
	)
}

type StatusWriter struct {
	m              sync.Mutex
	orderedFeedIDs []string
	feedIDToStatus map[string]*feedStatus
}

func NewStatusWriter(feeds []config.Feed) *StatusWriter {
	w := &StatusWriter{
		feedIDToStatus: map[string]*feedStatus{},
	}
	for _, feed := range feeds {
		w.orderedFeedIDs = append(w.orderedFeedIDs, feed.ID)
		w.feedIDToStatus[feed.ID] = &feedStatus{}
	}
	sort.Strings(w.orderedFeedIDs)
	for range w.orderedFeedIDs {
		fmt.Println()
	}
	w.refresh()
	return w
}

func (w *StatusWriter) SetNumArchives(feed *config.Feed, n int) {
	w.m.Lock()
	defer w.m.Unlock()
	w.feedIDToStatus[feed.ID].searched = true
	w.feedIDToStatus[feed.ID].numArchives = n
	w.refresh()
}

func (w *StatusWriter) RecordDownload(feed *config.Feed, err error) {
	// TODO: handle error case
	w.m.Lock()
	defer w.m.Unlock()
	w.feedIDToStatus[feed.ID].numDownloaded++
	w.refresh()
}

func (w *StatusWriter) RecordFinished(feed *config.Feed) {
	w.m.Lock()
	defer w.m.Unlock()
	w.feedIDToStatus[feed.ID].done = true
	w.refresh()
}

func (w *StatusWriter) refresh() {
	fmt.Printf("\r")
	for range w.orderedFeedIDs {
		fmt.Printf("\033[A\033[K")
	}
	for _, feedID := range w.orderedFeedIDs {
		fmt.Printf("%s: %s\n", feedID, w.feedIDToStatus[feedID])
	}
}

// RunOnceWithoutUnpacking retrieves remote data and stores it locally without
// unpacking the archives. That is, the compressed archive files are just stored.
func RunOnceWithoutUnpacking(session *actions.Session, writer *StatusWriter,
	start hour.Hour, end hour.Hour, targetAStore storage.WritableAStore) error {
	return run(
		session, writer, start, end,
		func(aFile storage.AFile) error {
			return storage.CopyAFile(session.RemoteAStore(), targetAStore, aFile)
		},
	)
}

// RunOnceWithUnpacking retrieves remote data, unpacks the archive, and stores it
// locally.
func RunOnceWithUnpacking(session *actions.Session, writer *StatusWriter,
	start hour.Hour, end hour.Hour, targetDStore storage.WritableDStore) error {
	return run(session, writer, start, end,
		func(aFile storage.AFile) error {
			return archive.Unpack(aFile, session.RemoteAStore(), targetDStore)
		},
	)
}

func run(session *actions.Session,
	writer *StatusWriter, start hour.Hour, end hour.Hour,
	fn func(file storage.AFile) error) error {
	searchResults, err := session.RemoteAStore().Search(&start, end)
	if err != nil {
		// TODO: notify status
		return err
	}
	var aFiles []storage.AFile
	for _, searchResult := range searchResults {
		for thisAFiles := range searchResult.AFiles {
			aFiles = append(aFiles, thisAFiles)
		}
	}
	writer.SetNumArchives(session.Feed(), len(aFiles))
	for _, aFile := range aFiles {
		writer.RecordDownload(session.Feed(), fn(aFile))
	}
	writer.RecordFinished(session.Feed())
	return nil
}
