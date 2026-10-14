package tasks

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
)

func TestSession_ConcurrentStoreInitialization(t *testing.T) {
	session := NewSession(
		&config.Feed{ID: "feed"},
		&config.Config{
			WorkspacePath: t.TempDir(),
			ObjectStorage: []config.ObjectStorage{{Endpoint: "localhost:9000", BucketName: "bucket"}},
		},
		slog.Default(),
		context.Background(),
		false,
	)

	const n = 20
	dStores := make([]storage.DStore, n)
	aStores := make([]storage.AStore, n)
	remoteAStores := make([]*astore.ReplicatedAStore, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			dStores[i] = session.LocalDStore()
			aStores[i] = session.LocalAStore()
			remoteAStores[i] = session.RemoteAStore()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 1; i < n; i++ {
		if dStores[i] != dStores[0] || aStores[i] != aStores[0] || remoteAStores[i] != remoteAStores[0] {
			t.Fatalf("Concurrent calls returned different stores")
		}
	}
}
