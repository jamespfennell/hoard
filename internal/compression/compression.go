package compression

import (
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/xz"
	"io"
)

type format struct {
	id           string
	extension    string
	minLevel     int
	maxLevel     int
	defaultLevel int
	newReader    func(r io.Reader) (io.ReadCloser, error)
	newWriter    func(w io.Writer, level int) io.WriteCloser
}

type Format struct {
	internal *format
}

var gzipFormat = format{
	id:           "gzip",
	extension:    "gz",
	minLevel:     gzip.BestSpeed,
	maxLevel:     gzip.BestCompression,
	defaultLevel: 6,  // the package uses -1 which doesn't fit well here
	newReader: func(r io.Reader) (io.ReadCloser, error) {
		return gzip.NewReader(r)
	},
	newWriter: func(w io.Writer, level int) io.WriteCloser {
		// The level is guaranteed to be correct, so the error can be ignored
		z, _ := gzip.NewWriterLevel(w, level)
		return z
	},
}

var Gzip = Format{internal: &gzipFormat}

var xzFormat = format{
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

var Xz = Format{internal: &xzFormat}

var allFormats = []Format{
	Gzip,
	Xz,
}

func (format *Format) Extension() string {
	return format.internal.extension
}

func (format *Format) String() string {
	return format.internal.id
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
		if format.internal.id == id {
			return format, true
		}
	}
	return Format{}, false
}

type Spec struct {
	Format Format
	Level  int
}

func (spec Spec) NewReader(r io.Reader) (io.ReadCloser, error) {
	return spec.Format.internal.newReader(r)
}

func (spec Spec) NewWriter(w io.Writer) io.WriteCloser {
	spec.fixLevel()
	return spec.Format.internal.newWriter(w, spec.Level)
}

func (spec *Spec) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawSpec struct {
		Format Format
		Level  *int
	}
	r := rawSpec{}
	if err := unmarshal(&r); err != nil {
		return err
	}
	spec.Format = r.Format
	if r.Level == nil {
		spec.Level = spec.Format.internal.defaultLevel
	} else {
		spec.Level = *r.Level
		if spec.fixLevel() {
			return fmt.Errorf("invalid compression level %d", *r.Level)
		}
	}
	return nil
}

func (spec *Spec) fixLevel() bool {
	if spec.Level < spec.Format.internal.minLevel {
		spec.Level = spec.Format.internal.minLevel
		return true
	}
	if spec.Level > spec.Format.internal.maxLevel {
		spec.Level = spec.Format.internal.maxLevel
		return true
	}
	return false
}
