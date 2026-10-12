package server

import (
	"testing"
	"time"
)

func TestApproximateDuration(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{0, "0 seconds"},
		{time.Second, "1 second"},
		{59 * time.Second, "59 seconds"},
		{time.Minute, "1 minute"},
		{3*time.Hour + 59*time.Minute, "3 hours"},
		{2*day + 5*time.Hour, "2 days"},
		{5*365*day + 100*day, "5 years"},
	} {
		if got := approximateDuration(tc.d); got != tc.want {
			t.Errorf("approximateDuration(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestPreciseDuration(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{42 * time.Second, "42 seconds"},
		{time.Minute + 30*time.Second, "1 minute"},
		{2*day + 14*time.Minute, "2 days and 14 minutes"},
		{2*day + 3*time.Hour + 14*time.Minute, "2 days, 3 hours and 14 minutes"},
		{400*day + time.Hour, "400 days and 1 hour"},
	} {
		if got := preciseDuration(tc.d); got != tc.want {
			t.Errorf("preciseDuration(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}
