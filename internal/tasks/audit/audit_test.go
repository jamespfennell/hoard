package audit

import (
	"bytes"
	"testing"

	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/tasks"
	"github.com/jamespfennell/hoard/internal/util/testutil"
)

var feed config.Feed
var hr = hour.Date(2020, 2, 2, 2)
var hr2 = hour.Date(2020, 2, 2, 3)
var aFile1 = storage.AFile{
	Prefix: "",
	Hour:   hr,
	Hash:   storage.ExampleHash(),
}
var aFile2 = storage.AFile{
	Prefix: "",
	Hour:   hr,
	Hash:   storage.ExampleHash2(),
}
var aFileWithXz = storage.AFile{
	Prefix:      "",
	Hour:        hr,
	Hash:        storage.ExampleHash2(),
	Compression: config.NewSpecWithLevel(config.Xz, 9),
}

func TestFindProblems_UnMergedHour(t *testing.T) {
	session := tasks.NewInMemorySession(&feed)
	aStore1 := session.RemoteAStore().Replicas()[0]
	testutil.ErrorOrFail(t, aStore1.Store(aFile1, bytes.NewReader(nil)))
	aStore2 := session.RemoteAStore().Replicas()[1]
	testutil.ErrorOrFail(t, aStore2.Store(aFile2, bytes.NewReader(nil)))

	problems, err := findProblems(session, &hr, hr, true, false)
	if err != nil {
		t.Errorf("unexpected error in findProblems: %s", err)
	}
	if len(problems) != 1 {
		t.Fatalf("unexpected number %d of problems; expected 1", len(problems))
	}
	problem := problems[0]
	unMergedHours, ok := problem.(unMergedHour)
	if !ok {
		t.Fatalf("expected unMergedHours problem; got %v", problem)
	}
	if unMergedHours.hour != hr {
		t.Fatalf("unexpected hour %s != %s", unMergedHours.hour, hr)
	}
}

func TestFindProblems_UnMergedHour_OutsideRange(t *testing.T) {
	session := tasks.NewInMemorySession(&feed)
	aStore1 := session.RemoteAStore().Replicas()[0]
	testutil.ErrorOrFail(t, aStore1.Store(aFile1, bytes.NewReader(nil)))
	aStore2 := session.RemoteAStore().Replicas()[1]
	testutil.ErrorOrFail(t, aStore2.Store(aFile2, bytes.NewReader(nil)))

	problems, err := findProblems(session, &hr2, hr2, true, false)
	if err != nil {
		t.Errorf("unexpected error in findProblems: %s", err)
	}
	if len(problems) != 0 {
		t.Fatalf("unexpected number %d of problems; expected 0", len(problems))
	}
}

func TestFindProblems_MissingData(t *testing.T) {
	session := tasks.NewInMemorySession(&feed)
	aStore1 := session.RemoteAStore().Replicas()[0]
	testutil.ErrorOrFail(t, aStore1.Store(aFile1, bytes.NewReader(nil)))

	problems, err := findProblems(session, &hr, hr, true, false)
	if err != nil {
		t.Errorf("unexpected error in findProblems: %s", err)
	}
	if len(problems) != 1 {
		t.Fatalf("unexpected number %d of problems; expected 1", len(problems))
	}
	problem := problems[0]
	missingDataForHours, ok := problem.(nonReplicatedData)
	if !ok {
		t.Fatalf("expected nonReplicatedData problem; got %v", problem)
	}
	if missingDataForHours.hour != hr {
		t.Fatalf("unexpected hour %s != %s", missingDataForHours.hour, hr)
	}
}

func TestFindProblems_MissingData_OutsideRange(t *testing.T) {
	session := tasks.NewInMemorySession(&feed)
	aStore1 := session.RemoteAStore().Replicas()[0]
	testutil.ErrorOrFail(t, aStore1.Store(aFile1, bytes.NewReader(nil)))

	problems, err := findProblems(session, &hr2, hr2, true, false)
	if err != nil {
		t.Errorf("unexpected error in findProblems: %s", err)
	}
	if len(problems) != 0 {
		t.Fatalf("unexpected number %d of problems; expected 0", len(problems))
	}
}

func TestFindProblems_IncorrectCompression(t *testing.T) {
	session := tasks.NewInMemorySession(&feed)
	testutil.ErrorOrFail(t, session.RemoteAStore().Store(aFileWithXz, bytes.NewReader(nil)))

	problems, err := findProblems(session, &hr, hr, true, true)
	if err != nil {
		t.Errorf("unexpected error in findProblems: %s", err)
	}
	if len(problems) != 1 {
		t.Fatalf("unexpected number %d of problems; expected 1", len(problems))
	}
	problem := problems[0]
	missingDataForHours, ok := problem.(incorrectCompression)
	if !ok {
		t.Fatalf("expected incorrectCompression problem; got %v", problem)
	}
	if missingDataForHours.hour != hr {
		t.Fatalf("unexpected hour %s != %s", missingDataForHours.hour, hr)
	}
}
