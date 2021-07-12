package persistence

import (
	"bytes"
	"fmt"
	"io"
)

type InMemoryPersistedStorage struct {
	keyIDToKey   map[string]Key
	keyIDToValue map[string][]byte
}

func NewInMemoryBytesStorage() *InMemoryPersistedStorage {
	return &InMemoryPersistedStorage{
		keyIDToKey:   map[string]Key{},
		keyIDToValue: map[string][]byte{},
	}
}

func (b *InMemoryPersistedStorage) Put(k Key, r io.Reader) error {
	v, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.keyIDToKey[k.id()] = k
	b.keyIDToValue[k.id()] = v
	return nil
}

func (b *InMemoryPersistedStorage) Get(k Key) (io.ReadCloser, error) {
	content, ok := b.keyIDToValue[k.id()]
	if !ok {
		return nil, fmt.Errorf("no such key %v", k)
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (b *InMemoryPersistedStorage) Delete(k Key) error {
	delete(b.keyIDToKey, k.id())
	delete(b.keyIDToValue, k.id())
	return nil
}

func (b *InMemoryPersistedStorage) Search(p Prefix) ([]SearchResult, error) {
	prefixIDToPrefix := map[string]SearchResult{}
	for _, k := range b.keyIDToKey {
		if !p.IsParent(k.Prefix) {
			continue
		}
		result := prefixIDToPrefix[k.Prefix.ID()]
		result.Prefix = k.Prefix
		result.Names = append(result.Names, k.Name)
		prefixIDToPrefix[k.Prefix.ID()] = result
	}
	var result []SearchResult
	for _, value := range prefixIDToPrefix {
		result = append(result, value)
	}
	return result, nil
}

func (b *InMemoryPersistedStorage) String() string {
	return "in memory"
}
