package audit

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
)

// TODO: tests
func Once(feed *config.Feed, fix bool, aStores []storage.AStore) error {
	// List all non-empty hours in all of them
	// List all non-empty hours in each object store
	// For each hour, list the files in there.
	// 	 If more than one in a given archive, run a merge on that hour
	// If the files in a hour differ across remote stores, move them across
	// If the non-empty hours are different, move them across
	remoteAStore := astore.NewMultiAStore(aStores...)
	allHours, err := remoteAStore.ListNonEmptyHours()
	if err != nil {
		return fmt.Errorf("failed to list hours for audit: %w", err)
	}
	var problems []problem
	hoursToMerge := map[storage.Hour]bool{}
	for _, hour := range allHours {
		if hour.NumAFiles <= 1 {
			continue
		}
		hoursToMerge[hour.Hour] = true
		problems = append(problems, unMergedHour{
			num:    hour.NumAFiles,
			hour:   hour.Hour,
			aStore: remoteAStore,
			feed:   feed,
		})
	}

	// list the contents of each hour and see if len(allHours) < 1
	//fmt.Println(hours)
	for _, aStore := range aStores {
		thisHours, err := aStore.ListNonEmptyHours()
		if err != nil {
			return fmt.Errorf("failed to list hours for audit: %w", err)
		}
		thisHoursSet := map[storage.Hour]bool{}
		for _, hour := range thisHours {
			thisHoursSet[hour.Hour] = true
		}
		for _, hour := range allHours {
			if !thisHoursSet[hour.Hour] {
				problems = append(problems, missingDataForHour{
					hour:           hour.Hour,
					source:         remoteAStore,
					target:         aStore,
					fixedByMerging: hoursToMerge[hour.Hour],
					feed:           feed,
				})
			}
		}
	}

	for _, p := range problems {
		fmt.Println(p)
	}
	return nil
}

type problem interface {
	Fix() error
	String() string
}

// Merge the archives for a given hour in the aggregate store
type unMergedHour struct {
	num    int
	hour   storage.Hour
	aStore storage.AStore
	feed   *config.Feed
}

func (p unMergedHour) Fix() error {
	return merge.DoHour(p.feed, p.aStore, p.hour)
}

func (p unMergedHour) String() string {
	return fmt.Sprintf("Hour %s for feed %s contains %d archive files and needs to be merged", p.hour, p.feed.ID, p.num)
}

// Move an archive from the aggregate store to an individual one
type missingDataForHour struct {
	hour           storage.Hour
	source         storage.AStore
	target         storage.AStore
	fixedByMerging bool
	feed           *config.Feed
}

func (p missingDataForHour) Fix() error {
	aFiles, err := p.source.ListInHour(p.hour)
	if err != nil {
		return err
	}
	for _, aFile := range aFiles {
		b, err := p.source.Get(aFile)
		if err != nil {
			return err // handle better
		}
		err = p.target.Store(aFile, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p missingDataForHour) String() string {
	return fmt.Sprintf("Missing data for feed %s in hour %s in remote store", p.feed.ID, p.hour)
}
