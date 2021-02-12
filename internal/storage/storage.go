package storage

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

const hashRegex = `(?P<hash>[a-z0-9]{12})`
const iso8601RegexHour = `(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})T(?P<hour>[0-9]{2})`
const iso8601RegexFull = iso8601RegexHour + `(?P<minute>\d{2})(?P<second>\d{2})\.(?P<millisecond>\d{3})Z`
const dFileStringRegex = `^(?P<prefix>.+?)` + iso8601RegexFull + `_` + hashRegex + `(?P<postfix>.+)$`

var dFileStringMatcher = regexp.MustCompile(dFileStringRegex)

type DFile struct {
	Prefix  string
	Postfix string
	Time    time.Time
	Hash    Hash
}

// TODO: test this
// String returns a string representation of the DFile. In Hoard, this string
// representation is always used as the DFile's file name when stored on disk.
func (d *DFile) String() string {
	var b strings.Builder
	b.WriteString(d.Prefix)
	b.WriteString(ISO8601(d.Time))
	b.WriteString("_")
	b.WriteString(string(d.Hash))
	b.WriteString(d.Postfix)
	return b.String()
}

// TODO: test this
// NewDFileFromString (re)constructs a DFile from a string representation of it; i.e.,
// from the output of the DFile String method.
func NewDFileFromString(s string) (DFile, bool) {
	match := dFileStringMatcher.FindStringSubmatch(s)
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
	if d.String() != s {
		return d, false
	}
	return d, true
}

func atoi(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

type DFileList []DFile

func (l DFileList) Len() int {
	return len(l)
}

func (l DFileList) Less(i, j int) bool {
	left := l[i]
	right := l[j]
	if left.Time != right.Time {
		return left.Time.Before(right.Time)
	}
	if left.Hash != right.Hash {
		return left.Hash < right.Hash
	}
	return left.Prefix <= right.Prefix
}

// Swap swaps the elements with indexes i and j.
func (l DFileList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type AFile struct {
	Prefix  string
	Postfix string
	Time    Hour
	Hash    Hash
}
