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
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		b, err := io.ReadAll(content)
		if err != nil {
			result.CopyErrors = append(result.CopyErrors, err)
			_ = content.Close()
			continue
		}
		if err := content.Close(); err != nil {
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		err = target.Store(dFile, b)
		if err != nil {
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		result.DFilesCopied = append(result.DFilesCopied, dFile)
		result.BytesCopied += len(b)
	}
	return result, nil
}
