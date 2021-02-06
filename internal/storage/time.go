package storage

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"strconv"
	"strings"
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

func PersistencePrefixToHour(p persistence.Prefix) (Hour, bool) {
	if len(p) != 4 {
		return Hour{}, false
	}
	t, err := time.Parse("2006-01-02-15", strings.Join(p, "-"))
	if err != nil {
		return Hour{}, false
	}
	return Hour(t), true
}

func DFileToPersistenceKey(d DFile) persistence.Key {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(d.Prefix)
	nameBuilder.WriteString(ISO8601(d.Time))
	nameBuilder.WriteString("_")
	nameBuilder.WriteString(string(d.Hash))
	nameBuilder.WriteString(d.Postfix)
	return persistence.Key{
		Prefix: []string{
			formatInt(d.Time.Year()),
			formatInt(int(d.Time.Month())),
			formatInt(d.Time.Day()),
			formatInt(d.Time.Hour()),
		},
		Name: nameBuilder.String(),
	}
}

func PersistenceKeyToDFile(k persistence.Key) (DFile, error) {
	return DFile{}, nil
}

func formatInt(i int) string {
	if i < 10 {
		return "0" + strconv.FormatInt(int64(i), 10)
	}
	return strconv.FormatInt(int64(i), 10)
}
