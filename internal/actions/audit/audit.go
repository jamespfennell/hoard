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
	"github.com/jamespfennell/hoard/config"
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
		session.Log().Warn("No remote object storage is configured, periodic auditor will not run")
		return
	}
	feed := session.Feed()
	session.Log().Info("Starting periodic auditor")
	ticker := util.NewPerHourTicker(1, 35*time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			start := hour.Now().Add(-24)
			err := RunOnce(session, &start, hour.Now(), true)
			if err != nil {
				session.Log().Errorf("Error while auditing: %s", err)
			}
			monitoring.RecordAudit(feed, err)
		case <-session.Ctx().Done():
			session.Log().Info("Stopped periodic auditor")
			return
		}
	}
}

// RunOnce runs the audit action once, optionally fixing problems it finds.
func RunOnce(session *actions.Session, startOpt *hour.Hour, end hour.Hour, fix bool) error {
	if session.RemoteAStore() == nil {
		session.Log().Error("Cannot audit because no remote object storage is configured")
		return fmt.Errorf("cannot audit because no remote object storage is configured")
	}
	feed := session.Feed()
	problems, err := findProblems(session, startOpt, end)
	if err != nil {
		return err
	}
	if len(problems) == 0 {
		session.Log().Info("No problems found during audit")
		return nil
	}
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "\nFound %d problem(s) for feed %s\n", len(problems), session.Feed().ID)
	for _, p := range problems {
		_, _ = fmt.Fprintf(&b, " - %s for hour %s", p.String(), p.Hour())
	}
	fmt.Println(b.String())
	if !fix {
		return fmt.Errorf("%s: found %d problem(s)\n", feed.ID, len(problems))
	}
	session.Log().Infof("Fixing %d problem(s) found during audit", len(problems))
	var errs []error
	for i, p := range problems {
		err := p.Fix()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to fix audit problem: %w", err))
			session.Log().Errorf("Failed to fix problem %d/%d: %s", i+1, len(problems), err)
			continue
		}
		session.Log().Infof("Fixed %d/%d problems\n", i+1-len(errs), len(problems))
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

	// First we look for unmerged hour problems.
	hoursToMerge := map[hour.Hour]bool{}
	for _, searchResult := range searchResults {
		if len(searchResult.AFiles) <= 1 {
			continue
		}
		hoursToMerge[searchResult.Hour] = true
		problems = append(problems, unMergedHour{problemBase{session, searchResult.Hour}})
	}

	// Then we look for non-replicated data problems.
	for _, aStore := range remoteAStore.Replicas() {
		subSearchResults, err := aStore.Search(startOpt, end)
		if err != nil {
			return nil, fmt.Errorf("failed to list hours for audit: %w", err)
		}
		thisHoursSet := map[hour.Hour]bool{}
		for _, searchResult := range subSearchResults {
			thisHoursSet[searchResult.Hour] = true
		}
		for _, searchResult := range searchResults {
			// Non-replicated data will automatically be replicated during merging, so we don't return a non-replicated
			// problem if the hour has a merging problem.
			if hoursToMerge[searchResult.Hour] {
				continue
			}
			if !thisHoursSet[searchResult.Hour] {
				problems = append(problems,
					nonReplicatedData{problemBase{session, searchResult.Hour}, aStore})
			}
		}
	}
	return problems, nil
}

type problem interface {
	Fix() error
	Feed() *config.Feed
	Hour() hour.Hour
	String() string
}

type problemBase struct {
	session *actions.Session
	hour    hour.Hour
}

func (p problemBase) Feed() *config.Feed {
	return p.session.Feed()
}

func (p problemBase) Hour() hour.Hour {
	return p.hour
}

type unMergedHour struct {
	problemBase
}

func (p unMergedHour) Fix() error {
	return merge.RunOnceForHour(p.session, p.session.RemoteAStore(), p.hour)
}

func (p unMergedHour) String() string {
	return "unmerged hour"
}

// Move an archive from the aggregate store to an individual one
type nonReplicatedData struct {
	problemBase
	target storage.AStore
}

func (p nonReplicatedData) Fix() error {
	aFiles, err := storage.ListAFilesInHour(p.session.RemoteAStore(), p.hour)
	if err != nil {
		return err
	}
	var errs []error
	for _, aFile := range aFiles {
		if err := storage.CopyAFile(p.session.RemoteAStore(), p.target, aFile); err != nil {
			errs = append(errs, err)
		}
	}
	return util.NewMultipleError(errs...)
}

func (p nonReplicatedData) String() string {
	return "non-replicated data"
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
