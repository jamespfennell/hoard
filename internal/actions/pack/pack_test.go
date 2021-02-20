package pack

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"testing"
	"time"
)

var b1 = []byte{50, 51, 52}
var b2 = []byte{60, 61, 62}
var h = storage.Hour(time.Date(2000, 1, 2, 3, 0, 0, 0, time.UTC))
var d1 = storage.DFile{
	Prefix:  "",
	Postfix: "",
	Time:    time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC),
	Hash:    storage.CalculateHash(b1),
}
var d2 = storage.DFile{
	Prefix:  "",
	Postfix: "",
	Time:    time.Date(2000, 1, 2, 3, 5, 5, 0, time.UTC),
	Hash:    storage.CalculateHash(b2),
}
var d3 = storage.DFile{
	Prefix:  "",
	Postfix: "",
	Time:    time.Date(2000, 1, 2, 3, 6, 5, 0, time.UTC),
	Hash:    storage.CalculateHash(b2),
}
var feed = &config.Feed{}

func TestPackHour(t *testing.T) {
	d := dstore.NewInMemoryDStore()
	errorOrFail(t, d.Store(d1, b1))
	errorOrFail(t, d.Store(d2, b2))
	errorOrFail(t, d.Store(d3, b2))

	a := astore.NewInMemoryAStore()

	errorOrFail(t, packHour(feed, d, a, h))

	// TODO
	// List all hours, ensure it's the hour we expect
	// List within the hour we expect, ensure there's only one result
	// Extract the AFile and deserialize it
	// Verify that it has 3 files and that the data is expected
}

func errorOrFail(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Unexpected error '%s'", err)
	}
}
