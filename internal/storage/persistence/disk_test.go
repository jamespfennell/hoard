package persistence

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
)

const root = "/hoard/workspace/downloads"

func newOnDiskByteStorageForTesting(filesMap map[string][]os.DirEntry) *OnDiskByteStorage {
	return &OnDiskByteStorage{
		root: root,
		readDir: func(s string) ([]os.DirEntry, error) {
			files, ok := filesMap[s]
			if !ok {
				return nil, fmt.Errorf("unknown directory '%s'", s)
			}
			return files, nil
		},
	}
}

type dirEntryForTesting struct {
	name  string
	isDir bool
}

func (f *dirEntryForTesting) Name() string {
	return f.name
}

func (f *dirEntryForTesting) IsDir() bool {
	return f.isDir
}

func (f *dirEntryForTesting) Type() os.FileMode {
	return 0
}

func (f *dirEntryForTesting) Info() (os.FileInfo, error) {
	return nil, nil
}

func TestOnDiskByteStorage_List(t *testing.T) {
	filesMap := make(map[string][]os.DirEntry)
	filesMap[path.Join(root, "a", "b")] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "c.ext",
			isDir: false,
		},
		&dirEntryForTesting{
			name:  "d.ext",
			isDir: false,
		},
		&dirEntryForTesting{
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
	filesMap := make(map[string][]os.DirEntry)
	filesMap[root] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "a",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a")] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "b",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a", "b")] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "d.ext",
			isDir: false,
		},
		&dirEntryForTesting{
			name:  "e",
			isDir: true,
		},
		&dirEntryForTesting{
			name:  "f",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a", "b", "e")] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "c.ext",
			isDir: false,
		},
	}
	filesMap[path.Join(root, "a", "b", "f")] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "g",
			isDir: true,
		},
	}
	filesMap[path.Join(root, "a", "b", "f", "g")] = []os.DirEntry{
		&dirEntryForTesting{
			name:  "h.ext",
			isDir: false,
		},
	}
	s := newOnDiskByteStorageForTesting(filesMap)

	expected := []NonEmptyPrefix{
		{
			[]string{"a", "b"},
			0,
		},
		{
			[]string{"a", "b", "e"},
			0,
		},
		{
			[]string{"a", "b", "f", "g"},
			0,
		},
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

func prefixListToMap(prefixes []NonEmptyPrefix) map[string]bool {
	m := make(map[string]bool)
	for _, p := range prefixes {
		m[p.Prefix.id()] = true
	}
	return m
}
