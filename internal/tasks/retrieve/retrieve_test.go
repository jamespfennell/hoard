package retrieve

import (
	"errors"
	"io"
	"testing"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/util/testutil"
)

var h = hour.Date(2000, 1, 2, 3)

// failingAStore is a WritableAStore that always fails to store.
type failingAStore struct{}

func (failingAStore) Store(storage.AFile, io.Reader) error {
	return errors.New("simulated store error")
}

func TestRunOnceWithoutUnpacking_ReturnsErrors(t *testing.T) {
	feed := &config.Feed{}
	session := tasks.NewInMemorySession(feed)
	testutil.CreateArchiveFromData(t, feed, session.RemoteAStore(), testutil.Data[0])

	err := RunOnceWithoutUnpacking(session, NewStatusWriter([]config.Feed{*feed}), h, h, failingAStore{})
	if err == nil {
		t.Errorf("Expected an error because the archive could not be stored")
	}
}
