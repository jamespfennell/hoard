package pack

import (
	"bytes"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/actions"
	"github.com/jamespfennell/hoard/internal/util/testutil"
	"testing"
)

var feed = &config.Feed{}

func TestPackHour(t *testing.T) {
	session := actions.NewInMemorySession(feed)

	data1 := testutil.Data[0]
	data2 := testutil.Data[1]
	data3 := testutil.Data[2]

	d := session.LocalDStore()
	errorOrFail(t, d.Store(data1.DFile, bytes.NewReader(data1.Content)))
	errorOrFail(t, d.Store(data2.DFile, bytes.NewReader(data2.Content)))
	errorOrFail(t, d.Store(data3.DFile, bytes.NewReader(data3.Content)))

	errorOrFail(t, packHour(session, data1.Hour))

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
