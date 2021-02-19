package persistence

import (
	"github.com/jamespfennell/hoard/internal/monitoring"
	"os"
	"path"
	"path/filepath"
	"time"
)

type OnDiskByteStorage struct {
	root    string
	readDir func(string) ([]os.DirEntry, error)
	remove  func(string) error
}

func NewOnDiskByteStorage(root string) *OnDiskByteStorage {
	return &OnDiskByteStorage{
		root:    path.Clean(root),
		readDir: os.ReadDir,
		remove:  os.Remove,
	}
}

func (b *OnDiskByteStorage) Put(k Key, v []byte) error {
	fullPath := path.Join(b.root, k.id())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	if err != nil {
		return err
	}
	return os.WriteFile(fullPath, v, 0666)
}

func (b *OnDiskByteStorage) Get(k Key) ([]byte, error) {
	fullPath := path.Join(b.root, k.id())
	return os.ReadFile(fullPath)
}

func (b *OnDiskByteStorage) Delete(k Key) error {
	fullPath := path.Join(b.root, k.id())
	err := b.remove(fullPath)
	if err != nil {
		return err
	}
	// We keep trying to remove empty directories until we can't
	for i := range k.Prefix {
		dirPath := path.Join(b.root, k.Prefix[:len(k.Prefix)-i].id())
		if err = b.remove(dirPath); err != nil {
			return nil
		}
	}
	return nil
}

func (b *OnDiskByteStorage) List(p Prefix) ([]Key, error) {
	fullPath := path.Join(b.root, p.id())
	files, err := b.readDir(fullPath)
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

func (b *OnDiskByteStorage) Search() ([]Prefix, error) {
	var result []Prefix
	return result, b.listSubPrefixes(Prefix{}, &result)
}

func (b *OnDiskByteStorage) listSubPrefixes(p Prefix, result *[]Prefix) error {
	// Note: the result is returned like this to avoid lots of memory
	// copying in each recursive call.
	fullPath := path.Join(b.root, p.id())
	files, err := b.readDir(fullPath)
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

func (b *OnDiskByteStorage) PeriodicallyReportUsageMetrics(label1, label2 string) {
	t := time.NewTicker(time.Minute)
	for {
		<-t.C
		var size int64
		var num int
		err := filepath.Walk(b.root, func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				num++
			}
			size += info.Size()
			return nil
		})
		if err != nil {
			continue
		}
		monitoring.RecordDiskUsage(label1, label2, num, size)
	}
}
