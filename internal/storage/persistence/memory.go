package persistence

import (
	"bytes"
	"fmt"
	"io"
)

type InMemoryByteStorage struct {
	keyIDToKey   map[string]Key
	keyIDToValue map[string][]byte
}

func NewInMemoryBytesStorage() *InMemoryByteStorage {
	return &InMemoryByteStorage{
		keyIDToKey:   map[string]Key{},
		keyIDToValue: map[string][]byte{},
	}
}

func (b *InMemoryByteStorage) Put(k Key, r io.Reader) error {
	v, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.keyIDToKey[k.id()] = k
	b.keyIDToValue[k.id()] = v
	return nil
}

func (b *InMemoryByteStorage) Get(k Key) (io.ReadCloser, error) {
	content, ok := b.keyIDToValue[k.id()]
	if !ok {
		return nil, fmt.Errorf("no such key %v", k)
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (b *InMemoryByteStorage) Delete(k Key) error {
	delete(b.keyIDToKey, k.id())
	delete(b.keyIDToValue, k.id())
	return nil
}

func (b *InMemoryByteStorage) Search(p Prefix) ([]SearchResult, error) {
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

func (b *InMemoryByteStorage) String() string {
	return "in memory"
}
