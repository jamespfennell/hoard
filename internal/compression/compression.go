package compression

import (
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/xz"
	"io"
)

type Format int

const (
	Gzip Format = 0
	Xz          = 1
)

const ExtensionRegex = `gz|xz`

var allFormats = []Format{
	Gzip,
	Xz,
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

var formatToImpl = map[Format]formatImpl{
	Gzip: gzipImpl,
	Xz:   xzImpl,
}

func (format *Format) impl() formatImpl {
	impl, ok := formatToImpl[*format]
	if ok {
		return impl
	}
	return gzipImpl
}

func (format *Format) Extension() string {
	return format.impl().extension
}

func (format *Format) String() string {
	return format.impl().id
}

func (format *Format) UnmarshalYAML(unmarshal func(interface{}) error) error {
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

func (format Format) MarshalYAML() (interface{}, error) {
	return format.String(), nil
}

func NewFormatFromId(id string) (Format, bool) {
	for _, format := range allFormats {
		if format.impl().id == id {
			return format, true
		}
	}
	return Gzip, false
}

func NewFormatFromExtension(extension string) (Format, bool) {
	for _, format := range allFormats {
		if format.impl().extension == extension {
			return format, true
		}
	}
	return Gzip, false
}

// Spec is an immutable type that specifies a compression format and level.
type Spec struct {
	Format Format
	Level  *int `yaml:",omitempty"`
}

// This is used as part of a hack to get different Spec instances that have the
// same format and same level setting to evaluate as equal using the built-in
// equality operator.
var intToPtr = map[int]*int{}

func NewSpecWithLevel(format Format, level int) Spec {
	if _, ok := intToPtr[level]; !ok {
		intToPtr[level] = &level
	}
	return Spec{
		Format: format,
		Level:  intToPtr[level],
	}
}

func (spec Spec) LevelActual() int {
	if spec.Level == nil {
		return spec.Format.impl().defaultLevel
	}
	return *spec.Level
}

func (spec Spec) Equal(other Spec) bool {
	return spec.LevelActual() == other.LevelActual() && spec.Format == other.Format
}

func (spec Spec) NewReader(r io.Reader) (io.ReadCloser, error) {
	return spec.Format.impl().newReader(r)
}

func (spec Spec) NewWriter(w io.Writer) io.WriteCloser {
	spec.fixLevel()
	return spec.Format.impl().newWriter(w, spec.LevelActual())
}

func (spec *Spec) fixLevel() bool {
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
