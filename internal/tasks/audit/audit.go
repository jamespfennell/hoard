// Package audit contains the audit task.
//
// This tasks searches for data problems in remote object storage.
// Currently, it looks for the following problems:
//   - Hours for which there a multiple archive files. These need to be merged.
//   - Data stored in one remote replica but not another. This data needs to be copied
//     to all replicas.
//
// The task optionally fixes the problems it encounters.
package audit

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/tasks/merge"
	"github.com/jamespfennell/hoard/internal/util"
)

type audit struct {
	enforceMerging bool
}

func New(enforceMerging bool) tasks.Task {
	return &audit{enforceMerging: enforceMerging}
}

func (a *audit) PeriodicTicker(session *tasks.Session) *util.Ticker {
	if session.RemoteAStore() == nil {
		session.Log().Warn("No remote object storage is configured, periodic auditor will not run")
		return nil
	}
	t := util.NewPerHourTicker(1, 35*time.Minute)
	return &t
}

func (a *audit) Run(session *tasks.Session) error {
	return nil
}

func (a *audit) Name() string {
	return "audit"
}

// RunPeriodically runs the audit task once every hour, at 35 minutes past the hour,
// and fixes any problems it encounters.
func RunPeriodically(session *tasks.Session, enforceMerging bool) {
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
			err := RunOnce(session, &start, hour.Now(), enforceMerging, false, true)
			if err != nil {
				session.Log().Error(fmt.Sprintf("Error while auditing: %s", err))
			}
			monitoring.RecordAudit(feed, err)
		case <-session.Ctx().Done():
			session.Log().Info("Stopped periodic auditor")
			return
		}
	}
}

// RunOnce runs the audit task once, optionally fixing problems it finds.
func RunOnce(session *tasks.Session, startOpt *hour.Hour, end hour.Hour, enforceMerging, enforceCompression, fix bool) error {
	if session.RemoteAStore() == nil {
		session.Log().Error("Cannot audit because no remote object storage is configured")
		return fmt.Errorf("cannot audit because no remote object storage is configured")
	}
	feed := session.Feed()
	problems, err := findProblems(session, startOpt, end, enforceMerging, enforceCompression)
	if err != nil {
		return err
	}
	if len(problems) == 0 {
		session.Log().Info("No problems found during audit")
		return nil
	}
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "\nFound %d problem(s) for feed %s\n", len(problems), session.Feed().ID)
	for i, p := range problems {
		_, _ = fmt.Fprintf(&b, " - %s for hour %s\n", p.String(), p.Hour())
		if i == 9 {
			_, _ = fmt.Fprintf(&b, " - and %d more problems...", len(problems)-10)
			break
		}
	}
	fmt.Println(b.String())
	if !fix {
		return fmt.Errorf("%s: found %d problem(s)\n", feed.ID, len(problems))
	}
	session.Log().Info(fmt.Sprintf("Fixing %d problem(s) found during audit", len(problems)))
	var errs []error
	for i, p := range problems {
		err := p.Fix()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to fix audit problem: %w", err))
			session.Log().Error(fmt.Sprintf("Failed to fix problem %d/%d: %s", i+1, len(problems), err))
			continue
		}
		session.Log().Info(fmt.Sprintf("Fixed %d/%d problems\n", i+1-len(errs), len(problems)))
	}
	return util.NewMultipleError(errs...)
}

func findProblems(session *tasks.Session, startOpt *hour.Hour, end hour.Hour, enforceMerging, enforceCompression bool) ([]problem, error) {
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
		// Even if not enforcing merging we populate this map because it's used when creating
		// non-replicated data problems.
		hoursToMerge[searchResult.Hour] = true
		if enforceMerging {
			problems = append(problems, unMergedHour{problemBase{session, searchResult.Hour}})
		}
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

	// Then incorrect compression problems
	if enforceCompression {
		for _, searchResult := range searchResults {
			if len(searchResult.AFiles) != 1 {
				continue
			}
			var aFile storage.AFile
			for aFileInSet := range searchResult.AFiles {
				aFile = aFileInSet
			}
			if !aFile.Compression.Equals(session.Feed().Compression) {
				problems = append(problems,
					incorrectCompression{problemBase{session, searchResult.Hour}, aFile})
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
	session *tasks.Session
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

type incorrectCompression struct {
	problemBase
	aFile storage.AFile
}

func (p incorrectCompression) Fix() error {
	_, err := archive.Recompress(p.session.Feed(), p.aFile, p.session.RemoteAStore(), p.session.RemoteAStore())
	if err != nil {
		return err
	}
	return p.session.RemoteAStore().Delete(p.aFile)
}

func (p incorrectCompression) String() string {
	return "incorrect compression"
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
