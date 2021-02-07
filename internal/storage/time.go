package storage

import (
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Hour time.Time

func (h Hour) MarshalJSON() ([]byte, error) {
	return time.Time(h).MarshalJSON()
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

func TimeToPersistencePrefix(t time.Time) persistence.Prefix {
	return []string{
		formatInt(t.Year()),
		formatInt(int(t.Month())),
		formatInt(t.Day()),
		formatInt(t.Hour()),
	}
}

func HourToPersistencePrefix(h Hour) persistence.Prefix {
	t := time.Time(h)
	return []string{
		formatInt(t.Year()),
		formatInt(int(t.Month())),
		formatInt(t.Day()),
	}
}

func DFileToPersistenceKey(d DFile) persistence.Key {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(d.Prefix)
	nameBuilder.WriteString(ISO8601(d.Time))
	nameBuilder.WriteString("_")
	nameBuilder.WriteString(string(d.Hash))
	nameBuilder.WriteString(d.Postfix)
	return persistence.Key{
		Prefix: TimeToPersistencePrefix(d.Time),
		Name:   nameBuilder.String(),
	}
}

func AFileToPersistenceKey(a AFile) persistence.Key {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(a.Prefix)
	nameBuilder.WriteString(ISO8601Hour(a.Time))
	nameBuilder.WriteString("_")
	nameBuilder.WriteString(string(a.Hash))
	nameBuilder.WriteString(".tar.gz")
	return persistence.Key{
		Prefix: HourToPersistencePrefix(a.Time),
		Name:   nameBuilder.String(),
	}
}

const hashRegex = `(?P<hash>[a-z0-9]{12})`
const iso8601RegexHour = `(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})T(?P<hour>[0-9]{2})`
const iso8601RegexFull = iso8601RegexHour + `(?P<minute>\d{2})(?P<second>\d{2})\.(?P<millisecond>\d{3})Z`
const dFileRegex = `^(?P<prefix>.+?)` + iso8601RegexFull + `_` + hashRegex + `(?P<postfix>.+)$`

var dFileMatcher = regexp.MustCompile(dFileRegex)

func PersistenceKeyToDFile(k persistence.Key) (DFile, bool) {
	match := dFileMatcher.FindStringSubmatch(k.Name)
	if match == nil {
		return DFile{}, false
	}
	d := DFile{
		Prefix: match[1],
		Time: time.Date(
			atoi(match[2]),
			time.Month(atoi(match[3])),
			atoi(match[4]),
			atoi(match[5]),
			atoi(match[6]),
			atoi(match[7]),
			atoi(match[8])*int(time.Millisecond),
			time.UTC,
		),
		Hash:    Hash(match[9]),
		Postfix: match[10],
	}
	// We validate the conversion by recomputing the key and ensuring it is the same.
	// This covers errors like the month value being out of range and the hour implied
	// by the prefix not matching the time in the file name
	if !DFileToPersistenceKey(d).Equals(k) {
		return d, false
	}
	return d, true
}

func atoi(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

func formatInt(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}
