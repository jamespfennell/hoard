package astore

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
)

type AStore interface {
	Store(aFile storage.AFile, content []byte) error
}

type ByteStorageBackedAStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedAStore(b persistence.ByteStorage) AStore {
	return ByteStorageBackedAStore{b: b}
}

func (a ByteStorageBackedAStore) Store(aFile storage.AFile, content []byte) error {
	fmt.Println("Writing", aFile)
	a.b.Put(storage.AFileToPersistenceKey(aFile), content)
	return nil
}
