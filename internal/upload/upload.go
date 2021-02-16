package upload

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/merge"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/workerpool"
)

func Once(f *config.Feed, localAStore storage.AStore, remoteAStore storage.AStore) error {
	aFiles, err := merge.Once(f, localAStore)
	if err != nil {
		// TODO: log the error?
	}
	var wg workerpool.ErrorGroup
	for _, aFile := range aFiles {
		wg.Add(1)
		content, err := localAStore.Get(aFile)
		if err != nil {
			// TODO: add the error to a return thing
			wg.Done(err)
			continue
		}
		err = remoteAStore.Store(aFile, content)
		if err != nil {
			// TODO: log and add the error
			wg.Done(err)
			continue
		}
		if err := localAStore.Delete(aFile); err != nil {
			return err
		}
		wg.Done(merge.DoHour(f, remoteAStore, aFile.Time))
	}
	return wg.Wait()
}
