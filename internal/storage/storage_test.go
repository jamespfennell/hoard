package storage

import (
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"testing"
	"time"
)

func TestPersistencePrefixToHour(t *testing.T) {
	p := persistence.Prefix{"2021", "02", "06", "22"}
	expected := Hour(time.Date(2021, 2, 6, 22, 0, 0, 0, time.UTC))

	actual, ok := PersistencePrefixToHour(p)

	if !ok {
		t.Fatal("Unexpected failure to convert prefix to hour", p)
	}
	if actual != expected {
		t.Error("Actual != expected; ", actual, expected)
	}
}

// TODO: error case tests?
