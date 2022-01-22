package persistence

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"strings"
	"time"
)

type Prefix []string

func (p Prefix) ID() string {
	return strings.Join(p, "/")
}

func (p Prefix) IsParent(p2 Prefix) bool {
	if len(p) > len(p2) {
		return false
	}
	for i := range p {
		if p[i] != p2[i] {
			return false
		}
	}
	return true
}

func EmptyPrefix() Prefix {
	return nil
}

type Key struct {
	Prefix Prefix
	Name   string
}

func (k Key) id() string {
	return k.Prefix.ID() + "/" + k.Name
}

func (k Key) Equals(k2 Key) bool {
	return k.id() == k2.id()
}

type SearchResult struct {
	Prefix Prefix
	Names  []string
}

// PersistedStorage is a place where bytes can be stored under a key.
type PersistedStorage interface {
	Put(k Key, reader io.Reader, t time.Time) error

	// TODO: audit all usages of this to ensure the reader is closed
	Get(k Key) (io.ReadCloser, error)

	// Delete deletes the bytes associated with the provided key.
	//
	// If the key does not exist in storage, nil is returned.
	Delete(k Key) error

	// Search returns a list of all prefixes such that there is at least one key in storage
	// with that prefix as a superprefix.
	Search(p Prefix) ([]SearchResult, error)

	// PeriodicallyReportUsageMetrics periodically reports Prometheus metrics
	// for the storage, until the context is cancelled.
	//
	// Each implementation can require certain labels, or ignore labels entirely.
	PeriodicallyReportUsageMetrics(ctx context.Context, labels ...string)

	fmt.Stringer
}

type verifyingStorage struct {
	PersistedStorage
}

// NewVerifyingStorage returns a wrapper around any PersistedStorage that performs
// data validation when Putting data.
//
// When streaming the data to the underlying PersistedStorage, the wrapper
// calculates an MD5 checksum of the data. After the underlying storage
// returns, it re-reads the data in full and verifies the checksums match.
//
// Methods other than Put invoke the underlying type directly.
func NewVerifyingStorage(backingStorage PersistedStorage) PersistedStorage {
	return verifyingStorage{backingStorage}
}

func (s verifyingStorage) Put(k Key, reader io.Reader, t time.Time) error {
	hasher := md5.New()
	tReader := io.TeeReader(reader, hasher)
	if err := s.PersistedStorage.Put(k, tReader, t); err != nil {
		return err
	}
	firstHash := hasher.Sum(nil)
	hasher.Reset()
	newReader, err := s.PersistedStorage.Get(k)
	if err != nil {
		return err
	}
	if _, err := io.Copy(hasher, newReader); err != nil {
		_ = newReader.Close()
		return err
	}
	if err := newReader.Close(); err != nil {
		return err
	}
	secondHash := hasher.Sum(nil)
	if !bytes.Equal(firstHash, secondHash) {
		return fmt.Errorf(
			"hash %s of stored data is not equal to the hash %s of retrieved data",
			string(firstHash), string(secondHash))
	}
	return nil
}

func (s verifyingStorage) String() string {
	return s.PersistedStorage.String() + " (with md5 verification)"
}
