package integrationtests

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/tests/deps"
	"io"
	"os"
	"reflect"
	"testing"
)

var minioServer1 = &deps.InProcessMinioServer{
	Port:     9000,
	User:     "hoard1",
	Password: "password1",
}

var minioServer2 = &deps.InProcessMinioServer{
	Port:     9001,
	User:     "hoard2",
	Password: "password2",
}

func TestMain(m *testing.M) {
	os.Mkdir(*hoardTmpDir, 0777)
	code := m.Run()
	minioServer1.CleanUp()
	minioServer2.CleanUp()
	os.Exit(code)
}

func Test_DownloadPackMerge(t *testing.T) {
	workspace := newFilesystem(t)
	server := newFeedServer(t)

	c := &config.Config{
		WorkspacePath: workspace.String(),
		Feeds: []config.Feed{
			{
				ID:      "feed1_",
				Postfix: ".txt",
				URL:     fmt.Sprintf("http://localhost:%d", server.Port()),
			},
		},
	}

	actions := []Action{
		Download,
		Download,
		Download,
		Download,
		Pack,
		Download,
		Download,
		Download,
		Download,
		Pack,
		Download,
		Download,
		Download,
		Download,
		Pack,
		Merge,
	}
	requireNilErr(t, ExecuteMany(actions, c))

	archivePaths, err := workspace.SubDir(hoard.ArchivesSubDir).ListAllFiles()
	if err != nil {
		t.Errorf("Error when listing all archive files: %s\n", err)
	}

	allContent := getAllContents(t, archivePaths, true)

	if !reflect.DeepEqual(allContent, server.Responses()) {
		t.Errorf(
			"Responses stored by Hoard: %v\nNot equal to responses sent by server: %v\n",
			allContent, server.Responses())
	}
}

func Test_DownloadUploadRetrieve(t *testing.T) {
	workspace := newFilesystem(t)
	server := newFeedServer(t)
	bucketName := newBucket(t, minioServer1)
	retrievePath := newFilesystem(t)

	c := &config.Config{
		WorkspacePath: workspace.String(),
		Feeds: []config.Feed{
			{
				ID:      "feed1_",
				Postfix: ".txt",
				URL:     fmt.Sprintf("http://localhost:%d", server.Port()),
			},
		},
		ObjectStorage: []config.ObjectStorage{
			minioServer1.Config(bucketName),
		},
	}

	actions := []Action{
		Download,
		Pack,
		Upload,
		Retrieve(retrievePath.String()),
	}
	requireNilErr(t, ExecuteMany(actions, c))

	archivePaths, err := retrievePath.ListAllFiles()
	if err != nil {
		t.Errorf("Error when listing all archive files: %s\n", err)
	}

	allContent := getAllContents(t, archivePaths, false)

	if !reflect.DeepEqual(allContent, server.Responses()) {
		t.Errorf(
			"Responses stored by Hoard: %v\nNot equal to responses sent by server: %v\n",
			allContent, server.Responses())
	}
}

// TODO:
//  test that uploads replicate data in two stores
//  test that audits fix problems with 2 remote stores
//  test that rebalancing works across 3 remote stores using audit
//  test vacate
//  test download upload files are in remote storage?

func getAllContents(t *testing.T, paths []string, packed bool) map[string]bool {
	allContent := map[string]bool{}
	for _, archivePath := range paths {
		b, err := os.ReadFile(archivePath)
		if err != nil {
			t.Errorf("Failed to read file %s; %s\n", archivePath, err)
			continue
		}
		if !packed {
			allContent[string(b)] = true
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
	return allContent
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

func newFilesystem(t *testing.T) deps.Filesystem {
	f, err := deps.NewFilesystem(*hoardTmpDir)
	cleanUp(t, f, err)
	return f
}

func newFeedServer(t *testing.T) *deps.FeedServer {
	s, err := deps.NewFeedServer()
	cleanUp(t, s, err)
	return s
}

func newBucket(t *testing.T, minioServer *deps.InProcessMinioServer) string {
	requireNilErr(t, minioServer.EnsureLaunched())
	bucketName, err := minioServer.NewBucket()
	requireNilErr(t, err)
	return bucketName
}

func cleanUp(t *testing.T, c interface{ CleanUp() error }, err error) {
	requireNilErr(t, err)
	t.Cleanup(func() {
		if err := c.CleanUp(); err != nil {
			if !*hoardOptionalCleanUp {
				t.Errorf("Failed to cleanup: %s", err)
			}
		}
	})
}

func requireNilErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
}
