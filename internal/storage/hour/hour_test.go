package hour

import (
	"testing"
	"time"
)

func TestFromTime(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("could not load America/New_York location")
	}
	nyTime := time.Date(2022, time.January, 22, 21, 49, 0, 0, loc)
	utcTime := time.Date(2022, time.January, 23, 2, 49, 0, 0, time.UTC)

	nyHour := FromTime(nyTime)
	utcHour := FromTime(utcTime)
	if nyHour != utcHour {
		t.Errorf("don't match: ny=%s, utc=%s", nyHour, utcHour)
	}
}
