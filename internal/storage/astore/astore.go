package astore

import "github.com/jamespfennell/hoard/internal/storage/persistence"

type AStore interface {
	// TODO
	// StoreAFile(aFile AFile, content []byte) error

	// ListNonEmptyHours() ([]time.Time, error)

	// ListInHour(hour time.Time) ([]DFile, error)
}

type ByteStorageBackedAStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedAStore(b persistence.ByteStorage) AStore {
	return ByteStorageBackedAStore{b: b}
}
