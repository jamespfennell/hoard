package storage

import (
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const hashRegex = `(?P<hash>[a-z0-9]{12})`
const iso8601RegexHour = `(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})T(?P<hour>[0-9]{2})`
const iso8601RegexFull = iso8601RegexHour + `(?P<minute>\d{2})(?P<second>\d{2})\.(?P<millisecond>\d{3})`
const optionalCompressionLevel = `(?P<level>_\d+)?`
const aFileExtension = `(?P<format>` + config.ExtensionRegex + `)`
const dFileStringRegex = `^(?P<prefix>.*?)` + iso8601RegexFull + `Z_` + hashRegex + `(?P<postfix>.*)$`
const aFileStringRegex = `^(?P<prefix>.*?)` + iso8601RegexHour + `Z_` + hashRegex + optionalCompressionLevel + `.tar.` + aFileExtension

var dFileStringMatcher = regexp.MustCompile(dFileStringRegex)
var aFileStringMatcher = regexp.MustCompile(aFileStringRegex)

type DFile struct {
	Prefix  string
	Postfix string
	Time    time.Time
	Hash    Hash
}

// String returns a string representation of the DFile. In Hoard, this string
// representation is always used as the DFile's file name when stored on disk.
func (d *DFile) String() string {
	t := d.Time
	iso8601 := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%03dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		(t.Nanosecond()/(1000*1000))%int(time.Millisecond),
	)
	var b strings.Builder
	b.WriteString(d.Prefix)
	b.WriteString(iso8601)
	b.WriteString("_")
	b.WriteString(string(d.Hash))
	b.WriteString(d.Postfix)
	return b.String()
}

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

// NewAFileFromString (re)constructs a AFile from a string representation of it; i.e.,
// from the output of the AFile String method.
func NewAFileFromString(s string) (AFile, bool) {
	match := aFileStringMatcher.FindStringSubmatch(s)
	if match == nil {
		return AFile{}, false
	}
	var spec config.Compression
	var legacyFileName bool
	if match[7] == "" {
		legacyFileName = true
		spec = config.NewSpecWithLevel(config.Gzip, 6)
	} else {
		format, ok := config.NewFormatFromExtension(match[8])
		if !ok {
			return AFile{}, false
		}
		spec = config.NewSpecWithLevel(format, atoi(match[7][1:]))
	}
	a := AFile{
		Prefix: match[1],
		Hour: hour.Date(
			atoi(match[2]),
			time.Month(atoi(match[3])),
			atoi(match[4]),
			atoi(match[5]),
		),
		Hash:        Hash(match[6]),
		Compression: spec,
	}
	// We validate the conversion by recomputing the key and ensuring it is the same.
	// This covers errors like the month value being out of range and the hour implied
	// by the prefix not matching the time in the file name
	if !legacyFileName && a.String() != s {
		return a, false
	}
	return a, true
}

func atoi(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

func Sort(dFiles []DFile) {
	sort.Sort(dFileList(dFiles))
}

// dFileList is a wrapper around a list of DFiles with implementations
// for the methods needed to sort using the std library.
type dFileList []DFile

func (l dFileList) Len() int {
	return len(l)
}

func (l dFileList) Less(i, j int) bool {
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
func (l dFileList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type AFile struct {
	Prefix      string
	Hour        hour.Hour
	Hash        Hash
	Compression config.Compression
}

// String returns a string representation of the AFile. In Hoard, this string
// representation is always used as the AFile's file name when stored on disk.
func (a AFile) String() string {
	var b strings.Builder
	b.WriteString(a.Prefix)
	b.WriteString(a.Hour.ISO8601())
	b.WriteString("_")
	b.WriteString(string(a.Hash))
	b.WriteString("_")
	_, _ = fmt.Fprintf(&b, "%d", a.Compression.LevelActual())
	b.WriteString(".tar.")
	b.WriteString(a.Compression.Format.Extension())
	return b.String()
}

// LegacyString returns a string representation of the AFile as it used to be in
// older versions of Hoard.
func (a AFile) LegacyString() string {
	var b strings.Builder
	b.WriteString(a.Prefix)
	b.WriteString(a.Hour.ISO8601())
	b.WriteString("_")
	b.WriteString(string(a.Hash))
	b.WriteString(".tar.gz")
	return b.String()
}

func (a AFile) Equals(other AFile) bool {
	return a.Prefix == other.Prefix &&
		a.Hour == other.Hour &&
		a.Hash == other.Hash &&
		a.Compression.Format == other.Compression.Format &&
		a.Compression.LevelActual() == a.Compression.LevelActual()
}

type SearchResult struct {
	Hour   hour.Hour
	AFiles map[AFile]bool
}

func NewAStoreSearchResult(hour hour.Hour) SearchResult {
	return SearchResult{
		Hour:   hour,
		AFiles: map[AFile]bool{},
	}
}

type WritableAStore interface {
	Store(aFile AFile, content io.Reader) error
}

type ReadableAStore interface {
	// TODO: audit all usages of this to ensure the reader is closed
	// TODO: ensure all implementers return nil on error
	Get(aFile AFile) (io.ReadCloser, error)

	// Searches for  all hours for which there is at least 1 AFile whose time is within that hour
	Search(startOpt *hour.Hour, end hour.Hour) ([]SearchResult, error)
}

type AStore interface {
	WritableAStore

	ReadableAStore

	Delete(aFile AFile) error

	fmt.Stringer
}

func ListAFilesInHour(aStore AStore, hour hour.Hour) ([]AFile, error) {
	searchResults, err := aStore.Search(&hour, hour)
	if err != nil {
		return nil, err
	}
	if len(searchResults) == 0 {
		return nil, nil
	}
	if len(searchResults) > 1 {
		return nil, fmt.Errorf("unexpected multiple search resutls for single hour: %v", searchResults)
	}
	aFiles := make([]AFile, 0, len(searchResults[0].AFiles))
	for aFile := range searchResults[0].AFiles {
		aFiles = append(aFiles, aFile)
	}
	return aFiles, nil
}

type ReadableDStore interface {
	// TODO: audit all usages of this to ensure the reader is closed
	// TODO: ensure all implementers return nil on error
	Get(dFile DFile) (io.ReadCloser, error)
}

type WritableDStore interface {
	Store(dFile DFile, content io.Reader) error
}

type DStore interface {
	ReadableDStore
	WritableDStore

	// Lists all hours for which there is at least 1 DFile whose time is within that hour
	ListNonEmptyHours() ([]hour.Hour, error)

	ListInHour(hour hour.Hour) ([]DFile, error)

	Delete(dFile DFile) error
}

type DStoreFactory interface {
	New() (DStore, func())
}

func CopyAFile(source AStore, target WritableAStore, aFile AFile) error {
	reader, err := source.Get(aFile)
	if err != nil {
		return err
	}
	err = target.Store(aFile, reader)
	if err != nil {
		_ = reader.Close()
		return err
	}
	return reader.Close()
}
