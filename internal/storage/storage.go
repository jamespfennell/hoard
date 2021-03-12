package storage

import (
	"fmt"
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

// String returns a string representation of the DFile. In Hoard, this string
// representation is always used as the DFile's file name when stored on disk.
func (a AFile) String() string {
	var b strings.Builder
	b.WriteString(a.Prefix)
	b.WriteString(a.Hour.ISO8601())
	b.WriteString("_")
	b.WriteString(string(a.Hash))
	b.WriteString(".tar.gz")
	return b.String()
}

// NewAFileFromString (re)constructs a AFile from a string representation of it; i.e.,
// from the output of the AFile String method.
func NewAFileFromString(s string) (AFile, bool) {
	match := aFileStringMatcher.FindStringSubmatch(s)
	if match == nil {
		return AFile{}, false
	}
	a := AFile{
		Prefix: match[1],
		Hour: hour.Date(
			atoi(match[2]),
			time.Month(atoi(match[3])),
			atoi(match[4]),
			atoi(match[5]),
		),
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
	Prefix string
	Hour   hour.Hour
	Hash   Hash
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
	Store(aFile AFile, content []byte) error
}

type AStore interface {
	WritableAStore

	// TODO: audit all usages of this to ensure the reader is closed
	Get(aFile AFile) (io.ReadCloser, error)

	// Searches for  all hours for which there is at least 1 AFile whose time is within that hour
	Search(startOpt *hour.Hour, end hour.Hour) ([]SearchResult, error)

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
	Get(dFile DFile) (io.ReadCloser, error)

	// Lists all hours for which there is at least 1 DFile whose time is within that hour
	ListNonEmptyHours() ([]hour.Hour, error)

	ListInHour(hour hour.Hour) ([]DFile, error)
}

type WritableDStore interface {
	Store(dFile DFile, content io.Reader) error
}

type DStore interface {
	ReadableDStore
	WritableDStore

	Delete(dFile DFile) error
}

type CopyResult struct {
	DFilesCopied []DFile
	CopyErrors   []error
	BytesCopied  int
}

// TODO: rename CopyDFiles
func Copy(source ReadableDStore, target WritableDStore, hour hour.Hour) (CopyResult, error) {
	result := CopyResult{}
	dFiles, err := source.ListInHour(hour)
	if err != nil {
		return result, err
	}
	if len(dFiles) == 0 {
		return result, nil
	}
	for _, dFile := range dFiles {
		content, err := source.Get(dFile)
		if err != nil {
			if content != nil {
				_ = content.Close()
			}
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		reader := &countingReader{internalReader: content}
		err = target.Store(dFile, reader)
		if err != nil {
			_ = content.Close()
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		if err := content.Close(); err != nil {
			result.CopyErrors = append(result.CopyErrors, err)
			continue
		}
		result.DFilesCopied = append(result.DFilesCopied, dFile)
		result.BytesCopied += reader.count
	}
	return result, nil
}

type countingReader struct {
	count          int
	internalReader io.Reader
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.internalReader.Read(p)
	r.count += n
	return n, err
}

func CopyAFile(source AStore, target AStore, aFile AFile) error {
	reader, err := source.Get(aFile)
	if err != nil {
		return err
	}
	// TODO: handle the error
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return target.Store(aFile, content)
}