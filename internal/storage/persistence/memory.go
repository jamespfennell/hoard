package persistence

import (
	"fmt"
)

type InMemoryByteStorage struct {
	keyIDToKey   map[string]Key
	keyIDToValue map[string][]byte
}

func NewInMemoryBytesStorage() InMemoryByteStorage {
	return InMemoryByteStorage{
		keyIDToKey:   map[string]Key{},
		keyIDToValue: map[string][]byte{},
	}
}

func (b InMemoryByteStorage) Put(k Key, v []byte) error {
	b.keyIDToKey[k.id()] = k
	b.keyIDToValue[k.id()] = v
	return nil
}

func (b InMemoryByteStorage) Get(k Key) ([]byte, error) {
	content, ok := b.keyIDToValue[k.id()]
	if !ok {
		return nil, fmt.Errorf("no such key %v", k)
	}
	return content, nil
}

func (b InMemoryByteStorage) Delete(k Key) error {
	delete(b.keyIDToKey, k.id())
	delete(b.keyIDToValue, k.id())
	return nil
}

func (b InMemoryByteStorage) List(p Prefix) ([]Key, error) {
	var keys []Key
	for _, key := range b.keyIDToKey {
		if key.Prefix.id() != p.id() {
			continue
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (b InMemoryByteStorage) Search(p Prefix) ([]SearchResult, error) {
	prefixIDToPrefix := map[string]SearchResult{}
	for _, k := range b.keyIDToKey {
		if !p.IsParent(k.Prefix) {
			continue
		}
		result := prefixIDToPrefix[k.Prefix.id()]
		result.Prefix = k.Prefix
		result.Names = append(result.Names, k.Name)
		prefixIDToPrefix[k.Prefix.id()] = result
	}
	var result []SearchResult
	for _, value := range prefixIDToPrefix {
		result = append(result, value)
	}
	return result, nil
}

func (b InMemoryByteStorage) String() string {
	return "in memory"
}
