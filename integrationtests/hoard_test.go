package integrationtests_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"time"

	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minioserver "github.com/minio/minio/cmd"
	// Import gateway
	_ "github.com/minio/minio/cmd/gateway"
)

// TODO: support running the integration tests through the CLI
//  in addition to the go package
func Test_OnceOperations(t *testing.T) {

	go func() {
		// TODO: configure the port
		// TODO: configure the file storage, probably in a tmp dir
		minioserver.Main([]string{"minio", "server", "tmp/minio"})
	}()
	// TODO: ping the server until it's available
	time.Sleep(2 * time.Second)
	// TODO: create the bucket
	// TODO: support leaving the test running afterwards so
	//  that the bucket storage can be tested
	client, _ := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	// TODO: variable bucket name? Or use config prefixes?
	client.MakeBucket(context.Background(), "test",
		minio.MakeBucketOptions{})

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
		ObjectStorage: []config.ObjectStorage{
			{
				Endpoint:"localhost:9000",
				AccessKey: "minioadmin",
				SecretKey: "minioadmin",
				BucketName: "test",
				Prefix: "hoard",
				Insecure: true,
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
	if err := hoard.Upload(c); err != nil {
		t.Fatalf("Failed to upload feeds: %s\n", err)
	}
	return

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
