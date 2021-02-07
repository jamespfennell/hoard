package persistence

import (
	"errors"
	"io/ioutil"
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
	fullPath := path.Join(b.root, k.id())
	return ioutil.ReadFile(fullPath)
}

func (b *onDiskByteStorage) Delete(k Key) error {
	// TODO
	return errors.New("not implemented")
}

func (b *onDiskByteStorage) List(p Prefix) ([]Key, error) {
	fullPath := path.Join(b.root, p.id())
	files, err := ioutil.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}
	var keys []Key
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		subP := make(Prefix, len(p))
		copy(subP, p)
		keys = append(keys, Key{
			Prefix: subP,
			Name:   file.Name(),
		})
	}
	return keys, nil
}

func (b *onDiskByteStorage) Search() ([]Prefix, error) {
	var result []Prefix
	return result, b.listSubPrefixes(Prefix{}, &result)
}

func (b *onDiskByteStorage) listSubPrefixes(p Prefix, result *[]Prefix) error {
	// Note: the result is returned like this to avoid lots of memory
	// copying in each recursive call.
	fullPath := path.Join(b.root, p.id())
	files, err := ioutil.ReadDir(fullPath)
	if err != nil {
		return err
	}
	dirHasRegularFile := false
	for _, file := range files {
		if !file.IsDir() {
			dirHasRegularFile = true
			continue
		}
		subP := make(Prefix, len(p)+1)
		copy(subP, p)
		subP[len(p)] = file.Name()
		if err := b.listSubPrefixes(subP, result); err != nil {
			return err
		}
	}
	if dirHasRegularFile {
		*result = append(*result, p)
	}
	return nil
}
