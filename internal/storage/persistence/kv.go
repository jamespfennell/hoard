package persistence

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/util"
)

// KVStore represents a place where bytes can be stored
type KVStore interface {
	Put(path string, content []byte) error

	Get(path string) ([]byte, error)

	// List(prefix string) ([]string, error)
}

type KVBackedDStore struct {
	kv KVStore
}

func NewKVBackedDStore(store KVStore) storage.DStore {
	return KVBackedDStore{kv: store}
}

func (dstore KVBackedDStore) StoreDFile(file storage.DFile, content []byte) error {
	return dstore.kv.Put(dstore.path(file), content)
}

func (dstore KVBackedDStore) path(file storage.DFile) string {
	return fmt.Sprintf("%04d/%02d/%02d/%02d/%s%s-%s%s",
		file.Time.Year(),
		file.Time.Month(),
		file.Time.Day(),
		file.Time.Hour(),
		file.Prefix,
		util.ISO8601(file.Time),
		file.Hash,
		file.Postfix,
		)
}
