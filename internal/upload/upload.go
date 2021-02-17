package upload

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/merge"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/util"
)

func Once(f *config.Feed, localAStore storage.AStore, remoteAStore storage.AStore) error {
	aFiles, err := merge.Once(f, localAStore)
	if err != nil {
		fmt.Printf(
			"Encountered error while merging local files: %s\n"+
				"Will continue with upload regardless\n", err)
	}
	var errs []error
	for _, aFile := range aFiles {
		err := uploadAFile(f, aFile, localAStore, remoteAStore)
		if err != nil {
			err = fmt.Errorf("Upload failed for %s, %w\n", aFile, err)
			fmt.Println(err)
			errs = append(errs, err)
		}
	}
	return util.NewMultipleError(errs...)
}

func uploadAFile(f *config.Feed, aFile storage.AFile, localAStore storage.AStore, remoteAStore storage.AStore) error {
	fmt.Printf("Beginning upload for %s\n", aFile)
	content, err := localAStore.Get(aFile)
	if err != nil {
		return err
	}
	if err := remoteAStore.Store(aFile, content); err != nil {
		return err
	}
	fmt.Printf("Finished upload for %v; attempting to merge remote archives\n", aFile)
	// The delete failing should not stop the merge from being attempted and vice-versa
	// so we run each operation irrespective of the result of the other.
	return util.NewMultipleError(
		localAStore.Delete(aFile),
		merge.DoHour(f, remoteAStore, aFile.Time),
	)
}
