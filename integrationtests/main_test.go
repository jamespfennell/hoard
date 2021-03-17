package integrationtests

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/integrationtests/external"
	"io"
	"os"
	"reflect"
	"testing"
)

var minioServer1 = external.InProcessMinioServer{
	Port:     9000,
	User:     "hoard1",
	Password: "password1",
}

var minioServer2 = external.InProcessMinioServer{
	Port:     9001,
	User:     "hoard2",
	Password: "password2",
}

func TestMain(m *testing.M) {
	//setup()
	fmt.Printf("Global setup")
	code := m.Run()
	// TODO: if the tests fail, sleep for sometime to enable inspection of
	//  the object storage
	// time.Sleep(50 * time.Second)
	//shutdown()
	minioServer1.CleanUp()
	minioServer2.CleanUp()
	os.Exit(code)
}

/*
func Test_OceOperations(t *testing.T) {
	minioServer1.EnsureLaunched()
	minioServer2.EnsureLaunched()
	t.Errorf("failed one")
	return
}*/

// TODO: support running the integration tests through the CLI
//  in addition to the go package
func Test_OnceOperations(t *testing.T) {
	workspace, err := external.NewFilesystem()
	requireNilErr(t, err)
	defer cleanUp(t, workspace)
	fmt.Printf("Using %s as the Hoard workspace\n", workspace)

	server, err := external.NewFeedServer()
	requireNilErr(t, err)
	defer cleanUp(t, server)
	fmt.Println("Running feed server on port", server.Port())

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
	for i, action := range actions {
		if err := action.ExecuteUsingPackage(c); err != nil {
			t.Fatalf("Failed for perform action %d: %s", i, err)
		}
	}

	archivePaths, err := workspace.SubDir(hoard.ArchivesSubDir).ListAllFiles()
	if err != nil {
		t.Errorf("Error when listing all archive files: %s\n", err)
	}

	allContent := getAllContents(t, archivePaths)

	if !reflect.DeepEqual(allContent, server.Responses()) {
		t.Errorf(
			"Responses stored by Hoard: %v\nNot equal to responses sent by server: %v\n",
			allContent, server.Responses())
	}
}

func getAllContents(t *testing.T, paths []string) map[string]bool {
	allContent := map[string]bool{}
	for _, archivePath := range paths {
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

func cleanUp(t *testing.T, c interface{ CleanUp() error }) {
	if err := c.CleanUp(); err != nil {
		t.Fatalf("Failed to clean up: %s", err)
	}
}

func requireNilErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
}
