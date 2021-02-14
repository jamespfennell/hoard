// Package astore contains implementations for different AStores used in Hoard
package astore

import (
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"strconv"
	"strings"
	"time"
)

type ByteStorageBackedAStore struct {
	b persistence.ByteStorage
}

func NewByteStorageBackedAStore(b persistence.ByteStorage) storage.AStore {
	return ByteStorageBackedAStore{b: b}
}

func (a ByteStorageBackedAStore) Store(aFile storage.AFile, content []byte) error {
	return a.b.Put(aFileToPersistenceKey(aFile), content)
}

func aFileToPersistenceKey(a storage.AFile) persistence.Key {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(a.Prefix)
	nameBuilder.WriteString(storage.ISO8601Hour(a.Time))
	nameBuilder.WriteString("_")
	nameBuilder.WriteString(string(a.Hash))
	nameBuilder.WriteString(".tar.gz")
	return persistence.Key{
		Prefix: hourToPersistencePrefix(a.Time),
		Name:   nameBuilder.String(),
	}
}

func hourToPersistencePrefix(h storage.Hour) persistence.Prefix {
	t := time.Time(h)
	return []string{
		formatInt(t.Year()),
		formatInt(int(t.Month())),
		formatInt(t.Day()),
		formatInt(t.Hour()),
	}
}

func formatInt(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}

type InMemoryAStore struct {
	aFileToContent map[storage.AFile][]byte
}

func NewInMemoryAStore() *InMemoryAStore {
	return &InMemoryAStore{
		aFileToContent: make(map[storage.AFile][]byte),
	}
}

func (a *InMemoryAStore) Store(aFile storage.AFile, content []byte) error {
	a.aFileToContent[aFile] = content
	return nil
}
