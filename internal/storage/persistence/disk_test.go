package persistence

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
)

const root = "/hoard/workspace/downloads"

func newOnDiskByteStorageForTesting(filesMap map[string][]os.DirEntry) *DiskPersistedStorage {
	return &DiskPersistedStorage{
		root: root,
		readDir: func(s string) ([]os.DirEntry, error) {
			files, ok := filesMap[s]
			if !ok {
				return nil, fmt.Errorf("unknown directory '%s'", s)
			}
			return files, nil
		},
		walkDir: func(root string, fn fs.WalkDirFunc) error {
			for dirPath, dirEntries := range filesMap {
				if !isParentPath(root, dirPath) {
					continue
				}
				if err := fn(dirPath, &dirEntryForTesting{isDir: true}, nil); err != nil {
					return err
				}
				for _, dirEntry := range dirEntries {
					if err := fn(filepath.Join(dirPath, dirEntry.Name()), dirEntry, nil); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
}

func isParentPath(parent, child string) bool {
	if len(parent) > len(child) {
		return false
	}
	for i, _ := range parent {
		if parent[i] != child[i] {
			return false
		}
	}
	return true
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

	allPossibleResults := []SearchResult{
		{
			[]string{"a", "b"},
			[]string{"d.ext"},
		},
		{
			[]string{"a", "b", "e"},
			[]string{"c.ext"},
		},
		{
			[]string{"a", "b", "f", "g"},
			[]string{"h.ext"},
		},
	}

	for i, testCase := range []struct {
		searchPrefix    Prefix
		expectedResults []SearchResult
	}{
		{
			searchPrefix:    EmptyPrefix(),
			expectedResults: allPossibleResults,
		},
		{
			searchPrefix:    []string{"a"},
			expectedResults: allPossibleResults,
		},
		{
			searchPrefix:    []string{"a", "b"},
			expectedResults: allPossibleResults,
		},
		{
			searchPrefix:    []string{"a", "b", "e"},
			expectedResults: []SearchResult{allPossibleResults[1]},
		},
		{
			searchPrefix:    []string{"a", "b", "f"},
			expectedResults: []SearchResult{allPossibleResults[2]},
		},
		{
			searchPrefix:    []string{"a", "b", "f", "g"},
			expectedResults: []SearchResult{allPossibleResults[2]},
		},
		{
			searchPrefix:    []string{"a", "c"},
			expectedResults: nil,
		},
	} {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual, err := s.Search(testCase.searchPrefix)
			if err != nil {
				t.Errorf("Unexpected error in Search method: %v", err)
			}
			if !reflect.DeepEqual(resultListToMap(testCase.expectedResults), resultListToMap(actual)) {
				t.Errorf("Unexpected prefixes: \n%v; expected\n%v", actual, testCase.expectedResults)
			}
		})
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

// resultListToMap is used to remove ordering from the result
func resultListToMap(results []SearchResult) map[string]SearchResult {
	m := make(map[string]SearchResult)
	for _, result := range results {
		m[result.Prefix.ID()] = result
	}
	return m
}
