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

// tmpFilePrefix is the prefix of temporary files that are used while writing data. Search ignores these files.
const tmpFilePrefix = ".hoard-tmp-"

type DiskPersistedStorage struct {
	root    string
	readDir func(string) ([]os.DirEntry, error)
	remove  func(string) error
	walkDir func(root string, fn fs.WalkDirFunc) error
}

func NewDiskPersistedStorage(root string) PersistedStorage {
	return NewVerifyingStorage(&DiskPersistedStorage{
		root:    path.Clean(root),
		readDir: os.ReadDir,
		remove:  os.Remove,
		walkDir: filepath.WalkDir,
	})
}

// Put writes the data to a temporary file in the same directory and then renames it to its final path. This
// ensures that the file at the final path is never partially written: either because writing failed, or because
// another task is reading the file while it is being written.
func (b *DiskPersistedStorage) Put(k Key, r io.Reader, t time.Time) error {
	fullPath := path.Join(b.root, k.id())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(path.Dir(fullPath), tmpFilePrefix)
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	// CreateTemp creates files with mode 0600; this matches the mode of files created by os.Create under the
	// usual umask.
	if err := file.Chmod(0644); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if _, err = io.Copy(file, r); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Chtimes(tmpPath, t, t); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func (b *DiskPersistedStorage) Get(k Key) (io.ReadCloser, error) {
	return os.Open(path.Join(b.root, k.id()))
}

func (b *DiskPersistedStorage) Delete(k Key) error {
	fullPath := path.Join(b.root, k.id())
	err := b.remove(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
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

func (b *DiskPersistedStorage) Search(parent Prefix) ([]SearchResult, error) {
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
		if strings.HasPrefix(d.Name(), tmpFilePrefix) {
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
		fullPrefix = append(fullPrefix, parent...)
		fullPrefix = append(fullPrefix, prefix...)
		result = append(result, SearchResult{
			Prefix: fullPrefix,
			Names:  idToNames[id],
		})
	}
	return result, nil
}

func (b *DiskPersistedStorage) PeriodicallyReportUsageMetrics(ctx context.Context, labels ...string) {
	var subDir string
	var feedID string
	if len(labels) >= 1 {
		subDir = labels[0]
	}
	if len(labels) >= 2 {
		feedID = labels[1]
	}
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
			monitoring.RecordDiskUsage(subDir, feedID, num, size)
		case <-ctx.Done():
			return
		}
	}
}

func (b *DiskPersistedStorage) String() string {
	return fmt.Sprintf("on disk mounted at %s", b.root)
}
