package persistence

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io/ioutil"
	"path"
)

type s3ObjectStorage struct {
	client     *minio.Client
	bucketName string
	prefix     string
}

// TODO: customize the base dir
// TODO: does this support multiple object storage backends?
// TODO: don't use the config object
func NewS3ObjectStorage(c config.ObjectStorage, prefix string) (ByteStorage, error) {
	storage := s3ObjectStorage{
		bucketName: c.BucketName,
		prefix:     prefix,
	}
	var err error
	storage.client, err = minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func (s s3ObjectStorage) Put(k Key, v []byte) error {
	// TODO: timeout on the context
	_, err := s.client.PutObject(
		context.Background(),
		s.bucketName,
		path.Join(s.prefix, k.id()),
		bytes.NewReader(v),
		int64(len(v)),
		minio.PutObjectOptions{}, // TODO: good options?
	)
	// TODO: wait 5 seconds given weak consistency
	return err
}

func (s s3ObjectStorage) Get(k Key) ([]byte, error) {
	object, err := s.client.GetObject(
		context.Background(),
		s.bucketName,
		path.Join(s.prefix, k.id()),
		minio.GetObjectOptions{},
	)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(object)
}

func (s s3ObjectStorage) Delete(k Key) error {
	return s.client.RemoveObject(
		context.Background(),
		s.bucketName,
		path.Join(s.prefix, k.id()),
		minio.RemoveObjectOptions{},
	)
	// TODO: wait 5 seconds given weak consistency
}

func (s s3ObjectStorage) List(p Prefix) ([]Key, error) {
	prefix := path.Join(s.prefix, p.id()) + "/"
	var keys []Key
	for object := range s.client.ListObjects(
		context.Background(),
		s.bucketName,
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
func (s s3ObjectStorage) Search() ([]Prefix, error) {
	// s.client.ListObjects()
	return nil, fmt.Errorf("S3ObjectStorage#%s not implemented\n", "Search")

}
