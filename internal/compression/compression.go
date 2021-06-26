package compression

import (
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/xz"
	"io"
)

type Format int

const (
	Gzip Format = iota
	Xz
)

func (format Format) Extension() string {
	switch format {
	case Xz:
		return "xz"
	case Gzip:
		fallthrough
	default:
		return "gz"
	}
}

func (format Format) NewReader(r io.Reader) (io.ReadCloser, error) {
	switch format {
	case Xz:
		return xz.NewReader(r), nil
	case Gzip:
		return gzip.NewReader(r)
	default:
		return nil, fmt.Errorf("unknown compression format %d", format)
	}
}

func NewFormatFromExtension(ext string) (Format, bool) {
	switch ext {
	case "xz":
		return Xz, true
	case "gz":
		fallthrough
	default:
		return Gzip, false
	}
}

type Spec struct {
	Format Format
	Level  int
}

func (spec Spec) NewReader(r io.Reader) (io.ReadCloser, error) {
	return spec.Format.NewReader(r)
}

func (spec Spec) NewWriter(w io.Writer) io.WriteCloser {
	switch spec.Format {
	case Xz:
		return xz.NewWriterLevel(w, spec.Level)
	case Gzip:
		fallthrough
	default:
		level := spec.Level
		if level < gzip.BestSpeed {
			level = gzip.BestSpeed
		}
		if level > gzip.BestCompression {
			level = gzip.BestCompression
		}
		// The level is guaranteed to be correct, so the error can be ignored
		w, _ := gzip.NewWriterLevel(w, level)
		return w
	}
}
