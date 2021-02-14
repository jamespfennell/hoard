package storage

type CopyResult struct {
	DFilesCopied []DFile
	CopyErrors   []error
	BytesCopied  int
}

// TODO: tests
func Copy(source ReadableDStore, target WritableDStore, hour Hour) (CopyResult, error) {
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
		err = target.Store(dFile, content)
		if err != nil {
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		result.DFilesCopied = append(result.DFilesCopied, dFile)
		result.BytesCopied += len(content)
	}
	return result, nil
}
