package hoard_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/testutil"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func Test_OnceOperations(t *testing.T) {
	workspace, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Failed to create temp dir for test: %s\n", err)
	}
	defer func() {
		err := os.RemoveAll(workspace)
		if err != nil {
			t.Errorf("Failed to clean up workspace: %s", err)
		}
	}()
	fmt.Printf("Using %s as the Hoard workspace\n", workspace)

	server, err := testutil.NewFeedServer()
	if err != nil {
		t.Fatalf("Failed to create feed server: %s\n", err)
	}
	defer func() {
		if err := server.Shutdown(); err != nil {
			t.Errorf("Failed to shut down feed server: %s", err)
		}
	}()
	fmt.Println("Running feed server on port", server.Port())

	c := &config.Config{
		WorkspacePath: workspace,
		Feeds: []config.Feed{
			{
				ID:      "feed1_",
				Postfix: ".txt",
				URL:     fmt.Sprintf("http://localhost:%d", server.Port()),
			},
		},
	}

	// We pack 3 times so that, by the pigeonhole principle, 2 of the archives
	// will be in the same hour. The merge code will then be tested correctly.
	if err := repeat(func() error { return hoard.Download(c) }, 4); err != nil {
		t.Fatalf("Failed to download feeds: %s\n", err)
	}
	if err := hoard.Pack(c); err != nil {
		t.Fatalf("Failed to pack feeds: %s\n", err)
	}
	if err := repeat(func() error { return hoard.Download(c) }, 4); err != nil {
		t.Fatalf("Failed to download feeds: %s\n", err)
	}
	if err := hoard.Pack(c); err != nil {
		t.Fatalf("Failed to pack feeds: %s\n", err)
	}
	if err := repeat(func() error { return hoard.Download(c) }, 4); err != nil {
		t.Fatalf("Failed to download feeds: %s\n", err)
	}
	if err := hoard.Pack(c); err != nil {
		t.Fatalf("Failed to pack feeds: %s\n", err)
	}
	if err := hoard.Merge(c); err != nil {
		t.Fatalf("Failed to merge feeds: %s\n", err)
	}

	var archivePaths []string
	if err := filepath.Walk(filepath.Join(workspace, hoard.ArchivesSubDir), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Errorf("Failed to read file: %s", path)
			return err
		}
		if info.IsDir() {
			return nil
		}
		archivePaths = append(archivePaths, path)
		return nil
	}); err != nil {
		t.Errorf("Error when walking the tree: %s\n", err)
	}

	allContent := map[string]bool{}
	for _, archivePath := range archivePaths {
		b, err := os.ReadFile(archivePath)
		if err != nil {
			t.Errorf("Failed to read file %s; %s\n", archivePath, err)
			continue
		}
		contents, err := extract(b)
		if err != nil {
			t.Errorf("Failed to extract %s; %s\n", archivePath, err)
			continue
		}
		for _, content := range contents {
			allContent[content] = true
			fmt.Println("Found content", content)
		}
	}

	if !reflect.DeepEqual(allContent, server.Responses()) {
		t.Errorf(
			"Responses stored by Hoard: %v\nNot equal to responses sent by server: %v\n",
			allContent, server.Responses())
	}
}

func extract(b []byte) ([]string, error) {
	var result []string
	gzr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var buffer bytes.Buffer
		if _, err = buffer.ReadFrom(tr); err != nil {
			return nil, err
		}
		if header.Name == hoard.ManifestFileName {
			continue
		}
		result = append(result, buffer.String())
	}
	return result, nil
}

func repeat(f func() error, n int) error {
	for i := 0; i < n; i++ {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}
