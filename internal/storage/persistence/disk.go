package persistence

import (
	"errors"
	"os"
	"path"
)

type onDiskByteStorage struct {
	root string
}

func NewOnDiskByteStorage(root string) ByteStorage {
	return &onDiskByteStorage{root: root}
}

func (b *onDiskByteStorage) Put(k Key, v []byte) error {
	fullPath := path.Join(b.root, k.id())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	if err != nil {
		return err
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	_, err = f.Write(v)
	if err != nil {
		// The write error takes precedence over the close error
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (b *onDiskByteStorage) Get(k Key) ([]byte, error) {
	// TODO
	return nil, errors.New("not implemented")
}

func (b *onDiskByteStorage) Delete(k Key) error {
	// TODO
	return errors.New("not implemented")
}

func (b *onDiskByteStorage) List(p Prefix) ([]Key, error) {
	// TODO
	return nil, errors.New("not implemented")
}

func (b *onDiskByteStorage) Search() ([]Prefix, error) {
	// TODO
	return nil, errors.New("not implemented")
}
