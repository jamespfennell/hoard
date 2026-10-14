package deps

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jamespfennell/hoard/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minioserver "github.com/minio/minio/cmd"
)

type InProcessMinioServer struct {
	Port     int
	User     string
	Password string
	path     string
	once     sync.Once
	// launchErr is the result of the first launch attempt. It is returned on every call to EnsureLaunched so that
	// a failed launch fails all tests that use the server, not just the first one.
	launchErr error
}

func (server *InProcessMinioServer) EnsureLaunched() error {
	server.once.Do(func() {
		server.launchErr = server.launch()
	})
	return server.launchErr
}

func (server *InProcessMinioServer) launch() error {
	if err := os.Setenv("MINIO_ROOT_USER", server.User); err != nil {
		return fmt.Errorf("failed to set env var MINIO_ROOT_USER: %w", err)
	}
	if err := os.Setenv("MINIO_ROOT_PASSWORD", server.Password); err != nil {
		return fmt.Errorf("failed to set env var MINIO_ROOT_PASSWORD: %w", err)
	}
	// We need to set a valid version for the Minio UI to work
	minioserver.Version = "2021-03-15T15:14:50Z"
	var err error
	server.path, err = os.MkdirTemp("", "hoard-minio-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir for object storage: %w", err)
	}
	go func() {
		minioserver.Main(
			[]string{"minio", "server", server.path,
				"--quiet", "--address", fmt.Sprintf(":%d", server.Port)})
	}()
	// Minio starts responding to HTTP requests before it is fully initialized; requests made in this window fail
	// with "Server not initialized yet". The cluster health endpoint only returns 200 once the server is ready.
	healthURL := fmt.Sprintf("http://localhost:%d/minio/health/cluster", server.Port)
	client := http.Client{Timeout: time.Second}
	pingT := time.NewTicker(50 * time.Millisecond)
	defer pingT.Stop()
	timeoutT := time.NewTimer(30 * time.Second)
	defer timeoutT.Stop()
	for {
		select {
		case <-pingT.C:
			res, err := client.Get(healthURL)
			if err != nil {
				continue
			}
			_ = res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		case <-timeoutT.C:
			return fmt.Errorf("object storage on port %d was not ready after 30 seconds", server.Port)
		}
	}
}

func (server *InProcessMinioServer) NewBucket() (string, error) {
	client, _ := minio.New(fmt.Sprintf("localhost:%d", server.Port), &minio.Options{
		Creds:  credentials.NewStaticV4(server.User, server.Password, ""),
		Secure: false,
	})
	name := fmt.Sprintf("bucket-%d", rand.Int())
	return name, client.MakeBucket(context.Background(), name, minio.MakeBucketOptions{})
}

func (server *InProcessMinioServer) CleanUp() {
	if server.path == "" {
		return
	}
	_ = os.RemoveAll(server.path)
}

func (server *InProcessMinioServer) Config(bucket string) config.ObjectStorage {
	return config.ObjectStorage{
		Endpoint:   fmt.Sprintf("localhost:%d", server.Port),
		AccessKey:  server.User,
		SecretKey:  server.Password,
		BucketName: bucket,
		Prefix:     "",
		Insecure:   true,
	}
}
