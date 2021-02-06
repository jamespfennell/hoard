package util

import (
	"fmt"
	"time"
)

type Hour time.Time

func ISO8601(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%03dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		(t.Nanosecond() / (1000 * 1000))%int(time.Millisecond),
	)
}

// TODO string -> time.Time
