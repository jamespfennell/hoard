package storage

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"strconv"
	"strings"
	"time"
)

type Hour time.Time

func (h Hour) String() string {
	t := time.Time(h)
	return fmt.Sprintf("%4d/%02d/%02d/%02d", t.Year(), t.Month(), t.Day(), t.Hour())
}

func (h Hour) PersistencePrefix() persistence.Prefix {
	t := time.Time(h)
	return []string{
		formatInt(t.Year()),
		formatInt(int(t.Month())),
		formatInt(t.Day()),
		formatInt(t.Hour()),
	}
}

func formatInt(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
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

func (h Hour) ISO8601() string {
	t := time.Time(h)
	return fmt.Sprintf("%04d%02d%02dT%02dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
	)
}

func NewHourFromPersistencePrefix(p persistence.Prefix) (Hour, bool) {
	if len(p) != 4 {
		return Hour{}, false
	}
	t, err := time.Parse("2006-01-02-15", strings.Join(p, "-"))
	if err != nil {
		return Hour{}, false
	}
	return Hour(t), true
}
