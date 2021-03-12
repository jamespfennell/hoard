package storage

import (
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"io"
)

type CopyResult struct {
	DFilesCopied []DFile
	CopyErrors   []error
	BytesCopied  int
}

func Copy(source ReadableDStore, target WritableDStore, hour hour.Hour) (CopyResult, error) {
	result := CopyResult{}
	dFiles, err := source.ListInHour(hour)
	if err != nil {
		return result, err
	}
	if len(dFiles) == 0 {
		return result, nil
	}
	for _, dFile := range dFiles {
		content, err := source.Get(dFile)
		if err != nil {
			if content != nil {
				_ = content.Close()
			}
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		reader := &countingReader{internalReader: content}
		err = target.Store(dFile, reader)
		if err != nil {
			_ = content.Close()
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		if err := content.Close(); err != nil {
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		result.DFilesCopied = append(result.DFilesCopied, dFile)
		result.BytesCopied += reader.count
	}
	return result, nil
}

type countingReader struct {
	count int
	internalReader io.Reader
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.internalReader.Read(p)
	r.count += n
	return n, err
}