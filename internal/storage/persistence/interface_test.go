package persistence_test

import (
	"bytes"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"io"
	"testing"
)

const data = "some sample data"

var key = persistence.Key{
	Prefix: persistence.Prefix{"one"},
	Name:   "two",
}

func TestVerifyingStorage_Success(t *testing.T) {
	backing := persistence.NewInMemoryPersistedStorage()
	verifying := persistence.NewVerifyingStorage(backing)

	testutil.ErrorOrFail(t, verifying.Put(key, bytes.NewBufferString(data)))
}

// corruptingStorage appends an additional string to all data it stores
type corruptingStorage struct {
	persistence.PersistedStorage
}

func (s corruptingStorage) Put(k persistence.Key, reader io.Reader) error {
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	b = append(b, []byte("extra stuff")...)
	return s.PersistedStorage.Put(k, bytes.NewReader(b))
}

// unreadableStorage always returns an error when Get is called
type unreadableStorage struct {
	persistence.PersistedStorage
}

func (s unreadableStorage) Get(k persistence.Key) (io.ReadCloser, error) {
	return nil, fmt.Errorf("random error")
}

// unclosableReader wraps a ReadCloser and always errors on Close
type unclosableReader struct {
	io.ReadCloser
}

func (r unclosableReader) Close() error {
	return fmt.Errorf("random error")
}

// storageWithUnclosableReaders returns unclosableReader on every Get
type storageWithUnclosableReaders struct {
	persistence.PersistedStorage
}

func (s storageWithUnclosableReaders) Get(k persistence.Key) (io.ReadCloser, error) {
	r, err := s.PersistedStorage.Get(k)
	return unclosableReader{r}, err
}

// unreadableReader wraps a ReadCloser and always errors on Read
type unreadableReader struct {
	io.ReadCloser
}

func (r unreadableReader) Read(b []byte) (int, error) {
	return 0, fmt.Errorf("random error")
}

// storageWithUnreadableReaders returns unreadableReader on every Get
type storageWithUnreadableReaders struct {
	persistence.PersistedStorage
}

func (s storageWithUnreadableReaders) Get(k persistence.Key) (io.ReadCloser, error) {
	r, err := s.PersistedStorage.Get(k)
	return unreadableReader{r}, err
}

func TestVerifyingStorage_FailureCase(t *testing.T) {
	cases := []persistence.PersistedStorage{
		corruptingStorage{persistence.NewInMemoryPersistedStorage()},
		unreadableStorage{persistence.NewInMemoryPersistedStorage()},
		storageWithUnclosableReaders{persistence.NewInMemoryPersistedStorage()},
		storageWithUnreadableReaders{persistence.NewInMemoryPersistedStorage()},
	}
	for i, storage := range cases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			verifying := persistence.NewVerifyingStorage(storage)
			err := verifying.Put(key, bytes.NewBufferString(data))
			if err == nil {
				t.Errorf("Expected an error! But didn't get any")
			}
		})
	}
}
