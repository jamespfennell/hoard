package audit

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/util"
	"math"
	"sort"
	"strings"
)

// TODO: tests
func Once(feed *config.Feed, fix bool, aStores []storage.AStore) error {
	problems, err := findProblems(feed, aStores)
	if err != nil {
		return err
	}
	for _, p := range problems {
		fmt.Println(p.String(true))
	}
	if len(problems) == 0 {
		fmt.Printf("%s: no problems found\n", feed.ID)
		return nil
	}
	if !fix {
		return fmt.Errorf("%s: found %d problems\n", feed.ID, len(problems))
	}
	fmt.Printf("%s: fixing %d problems\n", feed.ID, len(problems))
	var errs []error
	for i, p := range problems {
		err := p.Fix()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to fix audit problem: %w", err))
			fmt.Printf("%s: failed to fix problem %d/%d: %s\n", feed.ID, i+1, len(problems), err)
			continue
		}
		fmt.Printf("%s: fixed %d/%d problems\n", feed.ID, i+10-len(errs), len(problems))
	}
	return util.NewMultipleError(errs...)
}

func findProblems(feed *config.Feed, aStores []storage.AStore) ([]problem, error) {
	remoteAStore := astore.NewMultiAStore(aStores...)
	allHours, err := remoteAStore.ListNonEmptyHours()
	if err != nil {
		return nil, fmt.Errorf("failed to list hours for audit: %w", err)
	}
	var problems []problem
	hoursToMerge := map[storage.Hour]bool{}
	p := unMergedHours{
		aStore: remoteAStore,
		feed:   feed,
	}
	for _, hour := range allHours {
		if hour.NumAFiles() <= 1 {
			continue
		}
		hoursToMerge[hour.Hour()] = true
		p.hours = append(p.hours, hour)
	}
	if len(p.hours) > 0 {
		problems = append(problems, p)
	}

	// list the contents of each hour and see if len(allHours) < 1
	//fmt.Println(hours)
	for _, aStore := range aStores {
		p := missingDataForHours{
			source: remoteAStore,
			target: aStore,
			feed:   feed,
		}
		thisHours, err := aStore.ListNonEmptyHours()
		if err != nil {
			return nil, fmt.Errorf("failed to list hours for audit: %w", err)
		}
		thisHoursSet := map[storage.Hour]bool{}
		for _, hour := range thisHours {
			thisHoursSet[hour.Hour()] = true
		}
		for _, hour := range allHours {
			if !thisHoursSet[hour.Hour()] {
				p.hours = append(p.hours, hour.Hour())
			}
		}
		if len(p.hours) > 0 {
			problems = append(problems, p)
		}
	}
	return problems, nil
}

type problem interface {
	Fix() error
	String(verbose bool) string
}

type unMergedHours struct {
	hours  []storage.SearchResult
	aStore storage.AStore
	feed   *config.Feed
}

func (p unMergedHours) Fix() error {
	var errs []error
	for i, hour := range p.hours {
		err := merge.DoHour(p.feed, p.aStore, hour.Hour())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to merge during audit: %w", err))
			continue
		}
		fmt.Printf("%s: merged %d/%d unmerged hours\n", p.feed.ID, i+1-len(errs), len(p.hours))
	}
	return util.NewMultipleError(errs...)
}

func (p unMergedHours) String(verbose bool) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Feed %s has %d hours that need to be merged",
		p.feed.ID, len(p.hours)))
	if verbose {
		b.WriteString(":")
		var hours []storage.Hour
		for _, nonEmptyHour := range p.hours {
			hours = append(hours, nonEmptyHour.Hour())
		}
		b.WriteString(prettyPrintHours(hours, 6))
	}
	return b.String()
}

// Move an archive from the aggregate store to an individual one
type missingDataForHours struct {
	hours          []storage.Hour
	source         storage.AStore
	target         storage.AStore
	fixedByMerging bool
	feed           *config.Feed
}

func (p missingDataForHours) Fix() error {
	var errs []error
	for i, hour := range p.hours {
		aFiles, err := p.source.ListInHour(hour)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to populate data: %w", err))
			continue
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
		fmt.Printf("%s: populated data for %d/%d hours\n", p.feed.ID, i+1-len(errs), len(p.hours))
	}
	return util.NewMultipleError(errs...)
}

func (p missingDataForHours) String(verbose bool) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Feed %s has %d hours that are missing data in %s",
		p.feed.ID, len(p.hours), p.target))
	if verbose {
		b.WriteString(":")
		b.WriteString(prettyPrintHours(p.hours, 6))
	}
	return b.String()
}

func prettyPrintHours(hours []storage.Hour, numPerLine int) string {
	var b strings.Builder
	var cells []string
	for _, hour := range hours {
		cells = append(cells, hour.String())
	}
	sort.Strings(cells)
	for i := 0; i < int(math.Ceil(float64(len(cells))/float64(numPerLine))); i++ {
		b.WriteString("\n    ")
		for j := i * numPerLine; j < i*numPerLine+numPerLine && j < len(cells); j++ {
			b.WriteString(cells[j])
			b.WriteString(" ")
		}
	}
	return b.String()
}
