package persistence

import (
	"fmt"
	"io"
	"strings"
)

type Prefix []string

func (p Prefix) ID() string {
	return strings.Join(p, "/")
}

func (p Prefix) IsParent(p2 Prefix) bool {
	if len(p) > len(p2) {
		return false
	}
	for i, _ := range p {
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

// KVStore represents a place where bytes can be stored
type ByteStorage interface {
	Put(k Key, reader io.Reader) error

	Get(k Key) (io.ReadCloser, error)

	Delete(k Key) error

	// Search returns a list of all prefixes such that there is at least one key in storage
	// with that prefix as a superprefix.
	Search(p Prefix) ([]SearchResult, error)

	fmt.Stringer
}
