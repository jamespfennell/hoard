package storage

import (
	"fmt"
	"time"
)

type Hour time.Time

func (h Hour) String() string {
	t := time.Time(h)
	return fmt.Sprintf("%4d/%02d/%02d/%02d", t.Year(), t.Month(), t.Day(), t.Hour())
}

func (h Hour) MarshalJSON() ([]byte, error) {
	return time.Time(h).MarshalJSON()
}

func (h *Hour) UnmarshalJSON(b []byte) error {
	t := time.Time{}
	if err := t.UnmarshalJSON(b); err != nil {
		return err
	}
	*h = Hour(t)
	return nil
}

func ISO8601(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%03dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		(t.Nanosecond()/(1000*1000))%int(time.Millisecond),
	)
}

func ISO8601Hour(h Hour) string {
	t := time.Time(h)
	return fmt.Sprintf("%04d%02d%02dT%02dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
	)
}
