package persistence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jamespfennell/hoard/config"
)

// newObjectStorageForTesting returns object storage backed by a fake S3 server that responds to every request with
// the provided status code and S3 error code.
func newObjectStorageForTesting(t *testing.T, status int, errorCode string) PersistedStorage {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>` + errorCode +
			`</Code><Message>simulated error</Message></Error>`))
	}))
	t.Cleanup(server.Close)
	s, err := NewObjectPersistedStorage(
		context.Background(),
		&config.ObjectStorage{
			Endpoint:   strings.TrimPrefix(server.URL, "http://"),
			AccessKey:  "access",
			SecretKey:  "secret",
			BucketName: "bucket",
			Insecure:   true,
		},
		&config.Feed{ID: "feed"},
	)
	if err != nil {
		t.Fatalf("Failed to create object storage: %s", err)
	}
	return s
}

func TestObjectPersistedStorage_SearchReturnsListingError(t *testing.T) {
	s := newObjectStorageForTesting(t, http.StatusForbidden, "AccessDenied")

	results, err := s.Search(EmptyPrefix())
	if err == nil {
		t.Errorf("Expected an error when listing fails; got results %v", results)
	}
}

func TestObjectPersistedStorage_GetMissingObjectReturnsNilReader(t *testing.T) {
	s := newObjectStorageForTesting(t, http.StatusNotFound, "NoSuchKey")

	reader, err := s.Get(Key{Prefix: Prefix{"a"}, Name: "b"})
	if err == nil {
		t.Errorf("Expected an error for a missing object")
	}
	if reader != nil {
		t.Errorf("Expected a nil reader when Get fails")
	}
}
