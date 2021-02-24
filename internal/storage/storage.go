package storage

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const hashRegex = `(?P<hash>[a-z0-9]{12})`
const iso8601RegexHour = `(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})T(?P<hour>[0-9]{2})`
const iso8601RegexFull = iso8601RegexHour + `(?P<minute>\d{2})(?P<second>\d{2})\.(?P<millisecond>\d{3})Z`
const dFileStringRegex = `^(?P<prefix>.*?)` + iso8601RegexFull + `_` + hashRegex + `(?P<postfix>.*)$`
const aFileStringRegex = `^(?P<prefix>.*?)` + iso8601RegexHour + `Z_` + hashRegex + `(?P<postfix>.*)$`

var dFileStringMatcher = regexp.MustCompile(dFileStringRegex)
var aFileStringMatcher = regexp.MustCompile(aFileStringRegex)

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
		fmt.Println("No match :(")
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

// TODO: test this
// String returns a string representation of the DFile. In Hoard, this string
// representation is always used as the DFile's file name when stored on disk.
func (a AFile) String() string {
	var b strings.Builder
	b.WriteString(a.Prefix)
	b.WriteString(ISO8601Hour(a.Hour))
	b.WriteString("_")
	b.WriteString(string(a.Hash))
	b.WriteString(".tar.gz")
	return b.String()
}

// TODO: test this
// NewAFileFromString (re)constructs a AFile from a string representation of it; i.e.,
// from the output of the AFile String method.
func NewAFileFromString(s string) (AFile, bool) {
	match := aFileStringMatcher.FindStringSubmatch(s)
	if match == nil {
		return AFile{}, false
	}
	a := AFile{
		Prefix: match[1],
		Hour: Hour(time.Date(
			atoi(match[2]),
			time.Month(atoi(match[3])),
			atoi(match[4]),
			atoi(match[5]),
			0,
			0,
			0,
			time.UTC,
		)),
		Hash: Hash(match[6]),
	}
	// We validate the conversion by recomputing the key and ensuring it is the same.
	// This covers errors like the month value being out of range and the hour implied
	// by the prefix not matching the time in the file name
	if a.String() != s {
		return a, false
	}
	return a, true
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
	Prefix string
	Hour   Hour
	Hash   Hash
}

type SearchResult struct {
	hour       Hour
	elementIDs map[string]bool
}

func NewSearchResult(hour Hour) SearchResult {
	return SearchResult{
		hour:       hour,
		elementIDs: map[string]bool{},
	}
}

func (result SearchResult) Hour() Hour {
	return result.hour
}

func (result SearchResult) Add(elementID string) {
	result.elementIDs[elementID] = true
}

func (result SearchResult) AddAll(other SearchResult) {
	for elementID := range other.elementIDs {
		result.elementIDs[elementID] = true
	}
}

func (result SearchResult) NumAFiles() int {
	return len(result.elementIDs)
}

type AStore interface {
	Store(aFile AFile, content []byte) error

	Get(aFile AFile) ([]byte, error)

	// TODO: rename search
	// Lists all hours for which there is at least 1 AFile whose time is within that hour
	ListNonEmptyHours() ([]SearchResult, error)

	// TODO: remove? A replace by search
	ListInHour(hour Hour) ([]AFile, error)

	Delete(aFile AFile) error

	fmt.Stringer
}

type ReadableDStore interface {
	Get(dFile DFile) ([]byte, error)

	// Lists all hours for which there is at least 1 DFile whose time is within that hour
	ListNonEmptyHours() ([]Hour, error)

	ListInHour(hour Hour) ([]DFile, error)
}

type WritableDStore interface {
	Store(dFile DFile, content []byte) error

	Delete(dFile DFile) error
}

type DStore interface {
	ReadableDStore
	WritableDStore
}
