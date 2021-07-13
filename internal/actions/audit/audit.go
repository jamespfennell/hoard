// Package audit contains the audit action.
//
// This actions searches for data problems in remote object storage.
// Currently it looks for the following problems:
// - Hours for which there a multiple archive files. These need to be merged.
// - Data stored in one remote replica but not another. This data needs to be copied
//   to all replicas.
//
// The action optionally fixes the problems it encounters.
package audit

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/actions/merge"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
	"math"
	"sort"
	"strings"
	"time"
)

// RunPeriodically runs the audit action once every hour, at 35 minutes past the hour,
// and fixes any problems it encounters.
func RunPeriodically(session *actions.Session) {
	if session.RemoteAStore() == nil {
		fmt.Println("No remote object storage is configured, periodic auditor stopping")
		return
	}
	feed := session.Feed()
	fmt.Printf("Starting periodic auditor for %s\n", feed.ID)
	ticker := util.NewPerHourTicker(1, 35*time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			start := hour.Now().Add(-24)
			err := RunOnce(session, &start, hour.Now(), true)
			if err != nil {
				fmt.Printf("Encountered error in periodic auditing: %s", err)
			}
			monitoring.RecordAudit(feed, err)
		case <-session.Ctx().Done():
			fmt.Printf("Stopped periodic auditor for %s\n", feed.ID)
			return
		}
	}
}

// RunOnce runs the audit action once, optionally fixing problems it finds.
func RunOnce(session *actions.Session, startOpt *hour.Hour, end hour.Hour, fix bool) error {
	if session.RemoteAStore() == nil {
		return fmt.Errorf("cannot audit because no remote object storage is configured")
	}
	feed := session.Feed()
	problems, err := findProblems(session, startOpt, end)
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

func findProblems(session *actions.Session, startOpt *hour.Hour, end hour.Hour) ([]problem, error) {
	remoteAStore := session.RemoteAStore()
	searchResults, err := remoteAStore.Search(startOpt, end)
	if err != nil {
		return nil, fmt.Errorf("failed to list hours for audit: %w", err)
	}
	var problems []problem
	hoursToMerge := map[hour.Hour]bool{}
	p := unMergedHours{
		session: session,
	}
	for _, searchResult := range searchResults {
		if len(searchResult.AFiles) <= 1 {
			continue
		}
		hoursToMerge[searchResult.Hour] = true
		p.hours = append(p.hours, searchResult.Hour)
	}
	if len(p.hours) > 0 {
		problems = append(problems, p)
	}

	// list the contents of each hour and see if len(allHours) < 1
	//fmt.Println(hours)
	for _, aStore := range remoteAStore.Replicas() {
		p := missingDataForHours{
			session: session,
			target:  aStore,
		}
		subSearchResults, err := aStore.Search(startOpt, end)
		if err != nil {
			return nil, fmt.Errorf("failed to list hours for audit: %w", err)
		}
		thisHoursSet := map[hour.Hour]bool{}
		for _, searchResult := range subSearchResults {
			thisHoursSet[searchResult.Hour] = true
		}
		for _, searchResult := range searchResults {
			if !thisHoursSet[searchResult.Hour] {
				p.hours = append(p.hours, searchResult.Hour)
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
	session *actions.Session
	hours   []hour.Hour
}

func (p unMergedHours) Fix() error {
	var errs []error
	for i, hr := range p.hours {
		err := merge.RunOnceForHour(p.session, p.session.RemoteAStore(), hr)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to merge during audit: %w", err))
			continue
		}
		fmt.Printf("%s: merged %d/%d unmerged hours\n", p.session.Feed().ID, i+1-len(errs), len(p.hours))
	}
	return util.NewMultipleError(errs...)
}

func (p unMergedHours) String(verbose bool) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Feed %s has %d hours that need to be merged",
		p.session.Feed().ID, len(p.hours)))
	if verbose {
		b.WriteString(":")
		var hours []hour.Hour
		for _, nonEmptyHour := range p.hours {
			hours = append(hours, nonEmptyHour)
		}
		b.WriteString(prettyPrintHours(hours, 6))
	}
	return b.String()
}

// Move an archive from the aggregate store to an individual one
type missingDataForHours struct {
	session        *actions.Session
	hours          []hour.Hour
	target         storage.AStore
	fixedByMerging bool
}

func (p missingDataForHours) Fix() error {
	var errs []error
	for i, hr := range p.hours {
		aFiles, err := storage.ListAFilesInHour(p.session.RemoteAStore(), hr)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to populate data: %w", err))
			continue
		}
		for _, aFile := range aFiles {
			if err := storage.CopyAFile(p.session.RemoteAStore(), p.target, aFile); err != nil {
				return err
			}
		}
		fmt.Printf("%s: populated data for %d/%d hours\n", p.session.Feed().ID, i+1-len(errs), len(p.hours))
	}
	return util.NewMultipleError(errs...)
}

func (p missingDataForHours) String(verbose bool) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Feed %s has %d hours that are missing data in %s",
		p.session.Feed().ID, len(p.hours), p.target))
	if verbose {
		b.WriteString(":")
		b.WriteString(prettyPrintHours(p.hours, 6))
	}
	return b.String()
}

func prettyPrintHours(hours []hour.Hour, numPerLine int) string {
	var b strings.Builder
	var cells []string
	for _, hr := range hours {
		cells = append(cells, hr.String())
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
