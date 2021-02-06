package persistence

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/util"
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

// KVStore represents a place where bytes can be stored
type ByteStorage interface {
	Put(k Key, v []byte) error

	Get(k Key) ([]byte, error)

	Delete(k Key) error

	List(p Prefix) ([]Key, error)

	Search() ([]Prefix, error)

	// TODO: disk utilization statistics? Maybe just on the on disk one
}

// TODO: move this to DStore
type ByteStorageBackedDStore struct {
	b ByteStorage
}

func NewByteStorageBackedDStore(b ByteStorage) storage.DStore {
	return ByteStorageBackedDStore{b: b}
}

func (dstore ByteStorageBackedDStore) StoreDFile(file storage.DFile, content []byte) error {
	return dstore.b.Put(dstore.key(file), content)
}

//func (dstore ByteStorageBackedDStore) ListNonEmptyHours() ([]util.Hour, error) {
//	return nil, nil
//}

func (dstore ByteStorageBackedDStore) key(file storage.DFile) Key {
	return Key{
		Prefix: []string{
			// TODO: better way here?
			// TODO: hour -> prefix function?
			fmt.Sprintf("%04d", file.Time.Year()),
			fmt.Sprintf("%02d", file.Time.Month()),
			fmt.Sprintf("%02d", file.Time.Day()),
			fmt.Sprintf("%02d", file.Time.Hour()),
		},
		Name: fmt.Sprintf("%s%s_%s%s", // TODO: this needs to be shared with the AStore
			file.Prefix,
			util.ISO8601(file.Time),
			file.Hash,
			file.Postfix,
		),
	}
}
