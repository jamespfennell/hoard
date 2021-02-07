package persistence

import (
	"strings"
)

type Prefix []string

func (p Prefix) id() string {
	return strings.Join(p, "/")
}

type Key struct {
	Prefix Prefix
	Name   string
}

func (k Key) id() string {
	return k.Prefix.id() + "/" + k.Name
}

func (k Key) Equals(k2 Key) bool {
	// TODO: improve
	return k.id() == k2.id()
}

// KVStore represents a place where bytes can be stored
type ByteStorage interface {
	Put(k Key, v []byte) error

	Get(k Key) ([]byte, error)

	Delete(k Key) error

	List(p Prefix) ([]Key, error)

	// Search returns a list of all prefixes such that there is at least one key in storage
	// with that prefix.
	Search() ([]Prefix, error)

	// TODO: disk utilization statistics? Maybe just on the on disk one
}
