package persistence

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/util"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"path"
	"strings"
	"time"
)

type RemoteObjectStorage struct {
	client *minio.Client
	config *config.ObjectStorage
	feed   *config.Feed
}

// TODO: pass the context
// TODO: does this support multiple object storage backends?
func NewRemoteObjectStorage(c *config.ObjectStorage, f *config.Feed) (RemoteObjectStorage, error) {
	storage := RemoteObjectStorage{
		config: c,
		feed:   f,
	}
	var err error
	storage.client, err = minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return RemoteObjectStorage{}, err
	}
	return storage, nil
}

func (s RemoteObjectStorage) Put(k Key, v []byte) error {
	// TODO: timeout on the context
	_, err := s.client.PutObject(
		context.Background(),
		s.config.BucketName,
		path.Join(s.config.Prefix, s.feed.ID, k.id()),
		bytes.NewReader(v),
		int64(len(v)),
		minio.PutObjectOptions{}, // TODO: good options?
	)
	// TODO: wait 5 seconds given weak consistency
	monitoring.RecordRemoteStorageUpload(s.config, s.feed, err, len(v))
	return err
}

func (s RemoteObjectStorage) Get(k Key) ([]byte, error) {
	object, err := s.client.GetObject(
		context.Background(),
		s.config.BucketName,
		path.Join(s.config.Prefix, s.feed.ID, k.id()),
		minio.GetObjectOptions{},
	)
	var b []byte
	if err == nil {
		b, err = io.ReadAll(object)
	}
	monitoring.RecordRemoteStorageDownload(s.config, s.feed, err, len(b))
	closeErr := object.Close()
	return b, util.NewMultipleError(err, closeErr)
}

func (s RemoteObjectStorage) Delete(k Key) error {
	return s.client.RemoveObject(
		context.Background(),
		s.config.BucketName,
		path.Join(s.config.Prefix, s.feed.ID, k.id()),
		minio.RemoveObjectOptions{},
	)
	// TODO: wait 5 seconds given weak consistency
}

func (s RemoteObjectStorage) List(p Prefix) ([]Key, error) {
	prefix := path.Join(s.config.Prefix, s.feed.ID, p.id()) + "/"
	var keys []Key
	for object := range s.client.ListObjects(
		context.Background(), // TODO, etc.
		s.config.BucketName,
		minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		},
	) {
		subP := make(Prefix, len(p))
		copy(subP, p)
		keys = append(keys, Key{
			Prefix: subP,
			Name:   object.Key[len(prefix):],
		})
	}
	return keys, nil
}

// Search returns a list of all prefixes such that there is at least one key in storage
// with that prefix.
func (s RemoteObjectStorage) Search() ([]NonEmptyPrefix, error) {
	prefixIDToPrefix := map[string]NonEmptyPrefix{}
	prefix := path.Join(s.config.Prefix, s.feed.ID) + "/"
	for object := range s.client.ListObjects(
		context.Background(),
		s.config.BucketName,
		minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		},
	) {
		pieces := strings.Split(object.Key[len(prefix):], "/")
		prefix := Prefix(pieces[:len(pieces)-1])
		result := prefixIDToPrefix[prefix.id()]
		result.Prefix = prefix
		result.Names = append(result.Names, pieces[len(pieces)-1])
		prefixIDToPrefix[prefix.id()] = result
	}
	var result []NonEmptyPrefix
	for _, value := range prefixIDToPrefix {
		result = append(result, value)
	}
	return result, nil
}

func (s RemoteObjectStorage) String() string {
	return fmt.Sprintf("remote object bucket %s at %s (prefix %s)",
		s.config.BucketName, s.config.Endpoint, s.config.Prefix)
}

func (s RemoteObjectStorage) PeriodicallyReportUsageMetrics() {
	prefix := path.Join(s.config.Prefix, s.feed.ID) + "/"
	t := util.NewTicker(5*time.Minute, 0)
	for {
		<-t.C
		start := time.Now()
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
		fmt.Printf("Took %s to calculate remote storage usage for feed %s in bucket %s\n",
			time.Now().Sub(start), s.feed.ID, s.config.BucketName)
	}
}
