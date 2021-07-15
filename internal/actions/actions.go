// Package actions contains the definition of the Session which is used in all of the Hoard actions.
//
// The actions themselves (download, merge, audit, etc.) are defined in subpackages.
package actions

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/sirupsen/logrus"
	"os"
	"path"
)

const DownloadsSubDir = "downloads"
const ArchivesSubDir = "archives"
const TmpSubDir = "tmp"

// Session contains all the necessary pieces for performing actions in Hoard. Each action takes
// the Session as an input parameter and then uses the pieces it needs.
//
// The Session is a per-feed construct. Hoard's simple model for taking advantage of multiple CPUs is to
// to run operations for each feed in separate goroutines. Each of these goroutines is provided with the
// Session corresponding to its feed.
type Session struct {
	feed             *config.Feed
	objectStorage    []config.ObjectStorage
	ctx              context.Context
	log              *logrus.Entry
	workspace        string
	enableMonitoring bool
	localDStore      storage.DStore
	localAStore      storage.AStore
	remoteAStore     *astore.ReplicatedAStore
}

// NewSession creates a new Session for production code.
//
// In this session, local stores are based on the filesystem, rooted at the provided workspace.
// The remote AStore is based on the remote object storage configured in the configuration file.
func NewSession(feed *config.Feed, objectStorage []config.ObjectStorage, ctx context.Context, workspace string, enableMonitoring bool) *Session {
	return &Session{
		feed:             feed,
		objectStorage:    objectStorage,
		ctx:              ctx,
		log:              logrus.WithField("feed", feed.ID),
		workspace:        workspace,
		enableMonitoring: enableMonitoring,
		localDStore:      nil,
		localAStore:      nil,
		remoteAStore:     nil,
	}
}

// NewInMemorySession creates a new session in which all data is stored in-memory.
// This session is used for testing.
func NewInMemorySession(feed *config.Feed) *Session {
	remoteAStore := astore.NewReplicatedAStore(
		astore.NewInMemoryAStore(), astore.NewInMemoryAStore())
	return &Session{
		feed:             feed,
		ctx:              nil,
		log:              logrus.WithField("feed", feed.ID),
		workspace:        "",
		enableMonitoring: false,
		localDStore:      dstore.NewInMemoryDStore(),
		localAStore:      astore.NewInMemoryAStore(),
		remoteAStore:     &remoteAStore,
	}
}

// Feed returns the feed for this session.
func (s *Session) Feed() *config.Feed {
	return s.feed
}

// Ctx returns the context for this session.
func (s *Session) Ctx() context.Context {
	return s.ctx
}

// Log returns an object used for logging in this session.
func (s *Session) Log() *logrus.Entry {
	return s.log
}

// LogWithHour returns an object used for logging information about a specific hour in this session
func (s *Session) LogWithHour(h hour.Hour) *logrus.Entry {
	return s.log.WithField("hour", h)
}

// LocalDStore returns the DStore based on the local filesystem.
func (s *Session) LocalDStore() storage.DStore {
	if s.localDStore == nil {
		store := persistence.NewDiskPersistedStorage(path.Join(s.workspace, DownloadsSubDir, s.feed.ID))
		if s.enableMonitoring {
			go store.PeriodicallyReportUsageMetrics(s.ctx, DownloadsSubDir, s.feed.ID)
		}
		s.localDStore = dstore.NewPersistedDStore(store)
	}
	return s.localDStore
}

// LocalAStore returns the AStore based on the local filesystem.
func (s *Session) LocalAStore() storage.AStore {
	if s.localAStore == nil {
		store := persistence.NewDiskPersistedStorage(path.Join(s.workspace, ArchivesSubDir, s.feed.ID))
		if s.enableMonitoring {
			go store.PeriodicallyReportUsageMetrics(s.ctx, ArchivesSubDir, s.feed.ID)
		}
		s.localAStore = astore.NewPersistedAStore(store)
	}
	return s.localAStore
}

// RemoteAStore returns the AStore based on remote object storage. The boolean return value is false
// if not object storage has been configured - in this case, the AStore will be nil.
func (s *Session) RemoteAStore() *astore.ReplicatedAStore {
	if s.remoteAStore == nil && len(s.objectStorage) > 0 {
		var remoteAStores []storage.AStore
		for _, objectStorage := range s.objectStorage {
			objectStorage := objectStorage
			a, err := persistence.NewObjectPersistedStorage(
				s.ctx,
				&objectStorage,
				s.feed,
			)
			if err != nil {
				fmt.Println("failed to initialize object storage: ", err)
				return nil
			}
			if s.enableMonitoring {
				go a.PeriodicallyReportUsageMetrics(s.ctx)
			}
			remoteAStores = append(remoteAStores, astore.NewPersistedAStore(a))
		}
		remoteAStore := astore.NewReplicatedAStore(remoteAStores...)
		s.remoteAStore = &remoteAStore
	}
	return s.remoteAStore
}

// TempDStore creates a new temporary DStore and returns its. The second return value is a closer function
// that must be invoked to clean up the DStore.
func (s *Session) TempDStore() (storage.DStore, func() error) {
	st, closer := s.tempPersistedStorage()
	return dstore.NewPersistedDStore(st), closer
}

// TempDStore creates a new temporary AStore and returns its. The second return value is a closer function
// that must be invoked to clean up the AStore.
func (s *Session) TempAStore() (storage.AStore, func() error) {
	st, closer := s.tempPersistedStorage()
	return astore.NewPersistedAStore(st), closer
}

func (s *Session) tempPersistedStorage() (persistence.PersistedStorage, func() error) {
	if s.workspace == "" {
		return persistence.NewInMemoryPersistedStorage(), nilErrorFunc
	}
	tmpDir, err := os.MkdirTemp(path.Join(s.workspace, TmpSubDir), "")
	if err != nil {
		fmt.Printf("Failed to create temporary disk storage: %s\nFalling back in in-memory\n", err)
		return persistence.NewInMemoryPersistedStorage(), nilErrorFunc
	}
	return persistence.NewDiskPersistedStorage(tmpDir), func() error {
		return os.RemoveAll(tmpDir)
	}
}

func nilErrorFunc() error {
	return nil
}
