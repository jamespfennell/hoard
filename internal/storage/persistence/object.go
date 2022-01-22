package persistence

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"path"
	"strings"
	"time"
)

type ObjectPersistedStorage struct {
	client *minio.Client
	config *config.ObjectStorage
	feed   *config.Feed
	ctx    context.Context
}

func NewObjectPersistedStorage(ctx context.Context, c *config.ObjectStorage, f *config.Feed) (PersistedStorage, error) {
	storage := ObjectPersistedStorage{
		config: c,
		feed:   f,
		ctx:    ctx,
	}
	var err error
	storage.client, err = minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: !c.Insecure,
	})
	if err != nil {
		return ObjectPersistedStorage{}, err
	}
	return NewVerifyingStorage(storage), nil
}

func (s ObjectPersistedStorage) Put(k Key, r io.Reader, _ time.Time) error {
	// Make this configurable
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().UTC().Add(30*time.Second))
	defer cancel()
	info, err := s.client.PutObject(
		ctx,
		s.config.BucketName,
		path.Join(s.config.Prefix, s.feed.ID, k.id()),
		r,
		-1, // TODO: write an integration test that catches this bug :(
		minio.PutObjectOptions{
			PartSize: 1024 * 1024 * 30, // TODO: good choice here?
			// DisableMultipart: true,
		},
	)
	// We sleep because object storage backends are not always strongly
	// consistent and we want to make sure future interactions with the backend
	// sees this change.
	// TODO: reenable
	// time.Sleep(2 * time.Second)
	monitoring.RecordRemoteStorageUpload(s.config, s.feed, err, int(info.Size))
	return err
}

type contextCloser struct {
	io.ReadCloser
	c context.CancelFunc
}

func (c *contextCloser) Close() error {
	c.c()
	return c.ReadCloser.Close()
}

func (s ObjectPersistedStorage) Get(k Key) (io.ReadCloser, error) {
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().UTC().Add(100*time.Second))
	object, err := s.client.GetObject(
		ctx,
		s.config.BucketName,
		path.Join(s.config.Prefix, s.feed.ID, k.id()),
		minio.GetObjectOptions{},
	)
	var result io.ReadCloser
	var size int64
	if err != nil {
		cancel()
		if object != nil {
			_ = object.Close()
		}
	} else {
		var info minio.ObjectInfo
		info, err = object.Stat()
		size = info.Size
		result = &contextCloser{
			ReadCloser: object,
			c:          cancel,
		}
	}
	monitoring.RecordRemoteStorageDownload(s.config, s.feed, err, int(size))
	return result, err
}

func (s ObjectPersistedStorage) Delete(k Key) error {
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().UTC().Add(10*time.Second))
	defer cancel()
	err := s.client.RemoveObject(
		ctx,
		s.config.BucketName,
		path.Join(s.config.Prefix, s.feed.ID, k.id()),
		minio.RemoveObjectOptions{},
	)
	// TODO: reenable
	// time.Sleep(2 * time.Second)
	// time.Sleep(2 * time.Second)
	return err
}

// Search returns a list of all prefixes such that there is at least one key in storage
// with that prefix.
func (s ObjectPersistedStorage) Search(p Prefix) ([]SearchResult, error) {
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().UTC().Add(10*time.Second))
	defer cancel()
	prefixIDToPrefix := map[string]SearchResult{}
	root := path.Join(s.config.Prefix, s.feed.ID) + "/"
	prefix := path.Join(s.config.Prefix, s.feed.ID, p.ID()) + "/"
	for object := range s.client.ListObjects(
		ctx,
		s.config.BucketName,
		minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		},
	) {
		if len(object.Key) < len(root) {
			fmt.Printf("Error: object key (%s) is not prefixed by root(%s)\n", object.Key, root)
			continue
		}
		pieces := strings.Split(object.Key[len(root):], "/")
		prefix := Prefix(pieces[:len(pieces)-1])
		result := prefixIDToPrefix[prefix.ID()]
		result.Prefix = prefix
		result.Names = append(result.Names, pieces[len(pieces)-1])
		prefixIDToPrefix[prefix.ID()] = result
	}
	var result []SearchResult
	for _, value := range prefixIDToPrefix {
		result = append(result, value)
	}
	return result, nil
}

func (s ObjectPersistedStorage) String() string {
	return fmt.Sprintf("remote object bucket %s at %s (prefix %s)",
		s.config.BucketName, s.config.Endpoint, s.config.Prefix)
}

func (s ObjectPersistedStorage) PeriodicallyReportUsageMetrics(ctx context.Context, labels ...string) {
	prefix := path.Join(s.config.Prefix, s.feed.ID) + "/"
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var count int64
			var size int64
			for object := range s.client.ListObjects(
				context.Background(),
				s.config.BucketName,
				minio.ListObjectsOptions{
					Prefix:    prefix,
					Recursive: true,
				},
			) {
				count += 1
				size += object.Size
			}
			monitoring.RecordRemoteStorageUsage(s.config, s.feed, count, size)
		case <-ctx.Done():
			return
		}
	}
}
