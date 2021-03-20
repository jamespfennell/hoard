package retrieve

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/archive"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
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

func WithoutUnpacking(f *config.Feed, remoteAStore storage.AStore,
	localAStore storage.WritableAStore, writer *StatusWriter,
	start hour.Hour, end hour.Hour) error {
	return run(
		f, remoteAStore, writer, start, end,
		func(aFile storage.AFile) error {
			return storage.CopyAFile(remoteAStore, localAStore, aFile)
		},
	)
}

func Regular(f *config.Feed, remoteAStore storage.AStore,
	localDStore storage.WritableDStore, writer *StatusWriter,
	start hour.Hour, end hour.Hour) error {
	fmt.Println("REGULAR")
	return run(
		f, remoteAStore, writer, start, end,
		func(aFile storage.AFile) error {
			content, err := remoteAStore.Get(aFile)
			if err != nil {
				return err
			}
			sourceDStore, err := archive.NewArchiveFromSerialization(content)
			if err != nil {
				return err
			}
			copyResult, err := storage.Copy(sourceDStore, localDStore, aFile.Hour)
			if err != nil {
				return err
			}
			return util.NewMultipleError(copyResult.CopyErrors...)
		},
	)
}

func run(f *config.Feed, remoteAStore storage.AStore,
	writer *StatusWriter, start hour.Hour, end hour.Hour,
	fn func(file storage.AFile) error) error {
	searchResults, err := remoteAStore.Search(&start, end)
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
	writer.SetNumArchives(f, len(aFiles))
	for _, aFile := range aFiles {
		writer.RecordDownload(f, fn(aFile))
	}
	writer.RecordFinished(f)
	return nil
}
