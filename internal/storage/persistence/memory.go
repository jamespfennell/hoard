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

func (b InMemoryByteStorage) Search() ([]NonEmptyPrefix, error) {
	prefixIDToPrefix := map[string]NonEmptyPrefix{}
	for _, k := range b.keyIDToKey {
		prefixIDToPrefix[k.Prefix.id()] = NonEmptyPrefix{
			Prefix:  k.Prefix,
			NumKeys: prefixIDToPrefix[k.Prefix.id()].NumKeys + 1,
		}
	}
	var result []NonEmptyPrefix
	for _, value := range prefixIDToPrefix {
		result = append(result, value)
	}
	return result, nil
}
