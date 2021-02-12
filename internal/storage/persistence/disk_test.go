package persistence

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"
)

const root = "/hoard/workspace/downloads"

func newOnDiskByteStorageForTesting(filesMap map[string][]os.FileInfo) *onDiskByteStorage {
	return &onDiskByteStorage{
		root: root,
		readDir: func(s string) ([]os.FileInfo, error) {
			files, ok := filesMap[s]
			if !ok {
				return nil, fmt.Errorf("unknown directory '%s'", s)
			}
			return files, nil
		},
	}
}

type fileForTesting struct {
	name  string
	isDir bool
}

func (f *fileForTesting) Name() string {
	return f.name
}
func (f *fileForTesting) Size() int64 {
	return 0
}
func (f *fileForTesting) Mode() os.FileMode {
	return 0
}
func (f *fileForTesting) ModTime() time.Time {
	return time.Unix(100, 0)
}

func (f *fileForTesting) IsDir() bool {
	return f.isDir
}
func (f *fileForTesting) Sys() interface{} {
	return nil
}

func TestOnDiskByteStorage_List(t *testing.T) {
	filesMap := make(map[string][]os.FileInfo)
	filesMap[path.Join(root, "a", "b")] = []os.FileInfo{
		&fileForTesting{
			name:  "c.ext",
			isDir: false,
		},
		&fileForTesting{
			name:  "d.ext",
			isDir: false,
		},
		&fileForTesting{
			name:  "e",
			isDir: true,
		},
	}
	expectedKeys := []Key{
		{
			Prefix: []string{"a", "b"},
			Name:   "c.ext",
		},
		{
			Prefix: []string{"a", "b"},
			Name:   "d.ext",
		},
	}
	s := newOnDiskByteStorageForTesting(filesMap)

	keys, err := s.List([]string{"a", "b"})

	if err != nil {
		t.Errorf("Unexpected error in List method: %v", err)
	}
	if !reflect.DeepEqual(expectedKeys, keys) {
		t.Errorf("Unexpected keys returns %v; expected %v", keys, expectedKeys)
	}
}

func TestOnDiskByteStorage_Search(t *testing.T) {
	filesMap := make(map[string][]os.FileInfo)
	filesMap[root] = []os.FileInfo{
		&fileForTesting{
			name:  "a",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a")] = []os.FileInfo{
		&fileForTesting{
			name:  "b",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a", "b")] = []os.FileInfo{
		&fileForTesting{
			name:  "d.ext",
			isDir: false,
		},
		&fileForTesting{
			name:  "e",
			isDir: true,
		},
		&fileForTesting{
			name:  "f",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a", "b", "e")] = []os.FileInfo{
		&fileForTesting{
			name:  "c.ext",
			isDir: false,
		},
	}
	filesMap[path.Join(root, "a", "b", "f")] = []os.FileInfo{
		&fileForTesting{
			name:  "g",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a", "b", "f", "g")] = []os.FileInfo{
		&fileForTesting{
			name:  "h.ext",
			isDir: false,
		},
	}
	s := newOnDiskByteStorageForTesting(filesMap)

	expected := []Prefix{
		[]string{"a", "b"},
		[]string{"a", "b", "e"},
		[]string{"a", "b", "f", "g"},
	}

	actual, err := s.Search()
	if err != nil {
		t.Errorf("Unexpected error in List method: %v", err)
	}
	if !reflect.DeepEqual(prefixListToMap(expected), prefixListToMap(actual)) {
		t.Errorf("Unexpected prefixes %v; expected %v", actual, expected)
	}
}

func TestOnDiskByteStorage_Delete(t *testing.T) {
	var numDeletes int
	s := newOnDiskByteStorageForTesting(nil)
	s.remove = func(p string) error {
		if p == path.Join(root, "a", "b", "c.ext") {
			numDeletes++
			return nil
		}
		if p == path.Join(root, "a", "b") {
			numDeletes++
			return nil
		}
		return fmt.Errorf("cannot delete path %s", p)
	}

	k := Key{
		Prefix: []string{"a", "b"},
		Name:   "c.ext",
	}
	err := s.Delete(k)
	if err != nil {
		t.Errorf("Unexpected error in Delete method: %v", err)
	}
	if numDeletes != 2 {
		t.Errorf("Unexpected number of deletes (%d) in Delete method; expected 2", numDeletes)
	}
}

func prefixListToMap(prefixes []Prefix) map[string]bool {
	m := make(map[string]bool)
	for _, p := range prefixes {
		m[p.id()] = true
	}
	return m
}
