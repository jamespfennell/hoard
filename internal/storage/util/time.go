package util

import (
	"fmt"
	"time"
)

func ISO8601(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02dT%02d%02d%02dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		// TODO: milliseconds
	)
}