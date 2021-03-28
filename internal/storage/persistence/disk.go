package persistence

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type OnDiskByteStorage struct {
	root    string
	readDir func(string) ([]os.DirEntry, error)
	remove  func(string) error
	walkDir func(root string, fn fs.WalkDirFunc) error
}

func NewOnDiskByteStorage(root string) *OnDiskByteStorage {
	return &OnDiskByteStorage{
		root:    path.Clean(root),
		readDir: os.ReadDir,
		remove:  os.Remove,
		walkDir: filepath.WalkDir,
	}
}

func (b *OnDiskByteStorage) Put(k Key, r io.Reader) error {
	fullPath := path.Join(b.root, k.id())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	if err != nil {
		return err
	}
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, r)
	// TODO: in this case should we delete the on disk file?
	return err
}

func (b *OnDiskByteStorage) Get(k Key) (io.ReadCloser, error) {
	return os.Open(path.Join(b.root, k.id()))
}

func (b *OnDiskByteStorage) Delete(k Key) error {
	fullPath := path.Join(b.root, k.id())
	err := b.remove(fullPath)
	if err != nil {
		return err
	}
	// We keep trying to remove empty directories until we can't
	for i := range k.Prefix {
		dirPath := path.Join(b.root, k.Prefix[:len(k.Prefix)-i].ID())
		if err = b.remove(dirPath); err != nil {
			return nil
		}
	}
	return nil
}

func (b *OnDiskByteStorage) Search(parent Prefix) ([]SearchResult, error) {
	rootPath := filepath.Join(b.root, parent.ID())
	idToPrefix := map[string]Prefix{}
	idToNames := map[string][]string{}
	err := b.walkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		relativePath := filepath.Dir(path[len(rootPath)+1:])
		prefix := Prefix{}
		if relativePath != "." {
			prefix = strings.Split(
				relativePath,
				string(filepath.Separator),
			)
		}
		idToPrefix[prefix.ID()] = prefix
		idToNames[prefix.ID()] = append(idToNames[prefix.ID()], d.Name())
		return nil
	})
	if err != nil {
		return nil, err
	}
	var result []SearchResult
	for id, prefix := range idToPrefix {
		var fullPrefix Prefix
		for _, piece := range parent {
			fullPrefix = append(fullPrefix, piece)
		}
		for _, piece := range prefix {
			fullPrefix = append(fullPrefix, piece)
		}
		result = append(result, SearchResult{
			Prefix: fullPrefix,
			Names:  idToNames[id],
		})
	}
	return result, nil
}

func (b *OnDiskByteStorage) PeriodicallyReportUsageMetrics(ctx context.Context, label1, label2 string) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
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
		case <-ctx.Done():
			return
		}
	}
}

func (b *OnDiskByteStorage) String() string {
	return fmt.Sprintf("on disk mounted at %s", b.root)
}
