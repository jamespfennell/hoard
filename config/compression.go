package config

import (
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/jamespfennell/xz"
)

type CompressionFormat int

const (
	Gzip CompressionFormat = 0
	Xz   CompressionFormat = 1
	Zstd CompressionFormat = 2
)

const ExtensionRegex = `gz|xz|zstd`

func AllCompressionFormats() []CompressionFormat {
	return []CompressionFormat{
		Gzip,
		Xz,
		Zstd,
	}
}

type formatImpl struct {
	id           string
	extension    string
	minLevel     int
	maxLevel     int
	defaultLevel int
	newReader    func(r io.Reader) (io.ReadCloser, error)
	newWriter    func(w io.Writer, level int) io.WriteCloser
}

var gzipImpl = formatImpl{
	id:           "gzip",
	extension:    "gz",
	minLevel:     gzip.BestSpeed,
	maxLevel:     gzip.BestCompression,
	defaultLevel: 6, // the package uses -1 which doesn't fit well here
	newReader: func(r io.Reader) (io.ReadCloser, error) {
		return gzip.NewReader(r)
	},
	newWriter: func(w io.Writer, level int) io.WriteCloser {
		// The level is guaranteed to be correct, so the error can be ignored
		z, _ := gzip.NewWriterLevel(w, level)
		return z
	},
}

var xzImpl = formatImpl{
	id:           "xz",
	extension:    "xz",
	minLevel:     xz.BestSpeed,
	maxLevel:     xz.BestCompression,
	defaultLevel: xz.DefaultCompression,
	newReader: func(r io.Reader) (io.ReadCloser, error) {
		return xz.NewReader(r), nil
	},
	newWriter: func(w io.Writer, level int) io.WriteCloser {
		return xz.NewWriterLevel(w, level)
	},
}

var zstdImpl = formatImpl{
	id:           "zstd",
	extension:    "zstd",
	minLevel:     zstd.BestSpeed,
	maxLevel:     zstd.BestCompression,
	defaultLevel: zstd.DefaultCompression,
	newReader: func(r io.Reader) (io.ReadCloser, error) {
		return zstd.NewReader(r), nil
	},
	newWriter: func(w io.Writer, level int) io.WriteCloser {
		return zstd.NewWriterLevel(w, level)
	},
}

var formatToImpl = map[CompressionFormat]formatImpl{
	Gzip: gzipImpl,
	Xz:   xzImpl,
	Zstd: zstdImpl,
}

func (format *CompressionFormat) impl() formatImpl {
	impl, ok := formatToImpl[*format]
	if ok {
		return impl
	}
	return gzipImpl
}

func (format *CompressionFormat) Extension() string {
	return format.impl().extension
}

func (format *CompressionFormat) String() string {
	return format.impl().id
}

func (format *CompressionFormat) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var id string
	if err := unmarshal(&id); err != nil {
		return err
	}
	parsedFormat, ok := NewFormatFromId(id)
	if !ok {
		return fmt.Errorf("unknown compression format %q", id)
	}
	*format = parsedFormat
	return nil
}

func (format CompressionFormat) MarshalYAML() (interface{}, error) {
	return format.String(), nil
}

func NewFormatFromId(id string) (CompressionFormat, bool) {
	for _, format := range AllCompressionFormats() {
		if format.impl().id == id {
			return format, true
		}
	}
	return Gzip, false
}

func NewFormatFromExtension(extension string) (CompressionFormat, bool) {
	for _, format := range AllCompressionFormats() {
		if format.impl().extension == extension {
			return format, true
		}
	}
	return Gzip, false
}

// Compression is an immutable type that specifies a compression format and level.
type Compression struct {
	Format CompressionFormat
	Level  *int `yaml:",omitempty"`
}

// This is used as part of a hack to get different Compression instances that have the
// same format and same level setting to evaluate as equal using the built-in
// equality operator.
var intToPtr = map[int]*int{}
var intToPtrM sync.Mutex

func NewSpecWithLevel(format CompressionFormat, level int) Compression {
	intToPtrM.Lock()
	defer intToPtrM.Unlock()
	if _, ok := intToPtr[level]; !ok {
		intToPtr[level] = &level
	}
	return Compression{
		Format: format,
		Level:  intToPtr[level],
	}
}

func (spec Compression) LevelActual() int {
	if spec.Level == nil {
		return spec.Format.impl().defaultLevel
	}
	return *spec.Level
}

func (spec Compression) Equal(other Compression) bool {
	return spec.LevelActual() == other.LevelActual() && spec.Format == other.Format
}

func (spec Compression) NewReader(r io.Reader) (io.ReadCloser, error) {
	return spec.Format.impl().newReader(r)
}

func (spec Compression) NewWriter(w io.Writer) io.WriteCloser {
	spec.fixLevel()
	return spec.Format.impl().newWriter(w, spec.LevelActual())
}

func (spec Compression) Equals(other Compression) bool {
	return spec.Format == other.Format &&
		spec.LevelActual() == other.LevelActual()
}

func (spec *Compression) fixLevel() bool {
	if spec.Level == nil {
		return false
	}
	if *spec.Level < spec.Format.impl().minLevel {
		*spec.Level = spec.Format.impl().minLevel
		return true
	}
	if *spec.Level > spec.Format.impl().maxLevel {
		*spec.Level = spec.Format.impl().maxLevel
		return true
	}
	return false
}
