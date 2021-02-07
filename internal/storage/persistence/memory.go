package persistence

import (
	"errors"
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
	// TODO
	return nil, errors.New("not implemented")
}

func (b InMemoryByteStorage) Delete(k Key) error {
	// TODO
	return errors.New("not implemented")
}

func (b InMemoryByteStorage) List(p Prefix) ([]Key, error) {
	// TODO
	return nil, errors.New("not implemented")
}

func (b InMemoryByteStorage) Search() ([]Prefix, error) {
	prefixIDToSeen := map[string]bool{}
	var prefixes []Prefix
	for _, k := range b.keyIDToKey {
		if prefixIDToSeen[k.Prefix.id()] {
			continue
		}
		prefixIDToSeen[k.Prefix.id()] = true
		prefixes = append(prefixes, k.Prefix)
	}
	return prefixes, nil
}
