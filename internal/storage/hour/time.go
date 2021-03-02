package hour

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

func (h Hour) ISO8601() string {
	t := time.Time(h)
	return fmt.Sprintf("%04d%02d%02dT%02dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
	)
}

func (h Hour) Before(h2 Hour) bool {
	return time.Time(h).Before(time.Time(h2))
}

// IsBetween is inclusive
func (h Hour) IsBetween(startOpt *Hour, end Hour) bool {
	if end.Before(h) {
		return false
	}
	if startOpt == nil {
		return true
	}
	return !h.Before(*startOpt)
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

func Now() Hour {
	return Hour(time.Now().UTC().Truncate(time.Hour))
}

// TODO: use everywhere instead of time.Date
func Date(year int, month time.Month, day, hour int) Hour {
	return Hour(time.Date(year, month, day, hour, 0, 0, 0, time.UTC))
}

func formatInt(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}
