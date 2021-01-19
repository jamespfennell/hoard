package persistence

import (
	"os"
	"path"
)

type onDiskKVStore struct {
	root string
}

func NewOnDiskKVStore(root string) KVStore {
	return &onDiskKVStore{root: root}
}

func (kv *onDiskKVStore) Put(filePath string, content []byte) error {
	fullPath := path.Join(kv.root, filePath)
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	if err != nil {
		return err
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	_, err = f.Write(content)
	if err != nil {
		// The write error takes precedence over the close error
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (kv *onDiskKVStore) Get(filePath string) ([]byte, error) {
	// TODO
	return nil, nil
}
