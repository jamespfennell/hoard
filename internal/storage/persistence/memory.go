package persistence

import (
	"fmt"
	"os"
	"strings"
)

type InMemoryKVStore struct {
	files map[string][]byte
	dirs  map[string]InMemoryKVStore
}

func NewInMemoryKVStore() InMemoryKVStore {
	return InMemoryKVStore{
		files: make(map[string][]byte),
		dirs: make(map[string]InMemoryKVStore),
	}
}

func (kv InMemoryKVStore) Put(filePath string, content []byte) error {
	fmt.Println("Storing", filePath)
	i := strings.Index(filePath, string(os.PathSeparator))
	// If there is no separator this is a file
	if i<0 {
		kv.files[filePath] = make([]byte, len(content))
		copy(kv.files[filePath], content)
		return nil
	}
	kv.dirs[filePath[:i]] = NewInMemoryKVStore()
	return kv.dirs[filePath[:i]].Put(filePath[i+1:], content)
}


func (kv InMemoryKVStore) Get(filePath string) ([]byte, error) {
	i := strings.Index(filePath, string(os.PathSeparator))
	// If there is no separator this is a file
	if i<0 {
		content, ok := kv.files[filePath]
		if !ok {
			return nil, os.ErrNotExist
		}
		return content, nil
	}
	return kv.dirs[filePath[:i]].Get(filePath[i+1:])
}

func (kv InMemoryKVStore) Count() int {
	c := len(kv.files)
	for _, dir := range kv.dirs {
		c += dir.Count()
	}
	return c
}
