package deps

import (
	"os"
	"path/filepath"
)

type Filesystem struct {
	root string
}

func NewFilesystem(prefix string) (Filesystem, error) {
	root, err := os.MkdirTemp(prefix, "hoard-workspace-")
	if err != nil {
		return Filesystem{}, err
	}
	return Filesystem{root: root}, nil
}

func (f Filesystem) String() string {
	return f.root
}

func (f Filesystem) CleanUp() error {
	if f.root == "" {
		return nil
	}
	return os.RemoveAll(f.root)
}

func (f Filesystem) SubDir(path string) Filesystem {
	return Filesystem{
		filepath.Join(f.root, path),
	}
}

func (f Filesystem) ListAllFiles() ([]string, error) {
	var paths []string
	err := filepath.Walk(f.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	return paths, err
}
