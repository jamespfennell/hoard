package archive

import (
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/xz"
	"io"
)

type Spec struct {
	BestSpeed          int
	BestCompression    int
	DefaultCompression int

	newWriterLevel func(w io.Writer, level int) io.WriteCloser
	NewReader      func(r io.Reader) io.ReadCloser
}

func (s Spec) NewWriterLevel(w io.Writer, level int) io.WriteCloser {
	if level < s.BestSpeed {
		level = s.BestSpeed
	}
	if level > s.BestCompression {
		level = s.BestCompression
	}
	return s.newWriterLevel(w, level)
}

func GetSpec(format config.CompressionFormat) (Spec, error) {
	switch format {
	case config.Gzip:
		return Spec{
			BestSpeed:          gzip.BestSpeed,
			BestCompression:    gzip.BestCompression,
			DefaultCompression: gzip.DefaultCompression,
			NewReader: func(r io.Reader) io.ReadCloser {
				z, _ := gzip.NewReader(r)
				return z
			},
			newWriterLevel: func(w io.Writer, level int) io.WriteCloser {
				z, _ := gzip.NewWriterLevel(w, level)
				return z
			},
		}, nil
	case config.Xz:
		return Spec{
			BestSpeed:          xz.BestSpeed,
			BestCompression:    xz.BestCompression,
			DefaultCompression: xz.DefaultCompression,
			NewReader: func(r io.Reader) io.ReadCloser {
				return xz.NewReader(r)
			},
			newWriterLevel: func(w io.Writer, level int) io.WriteCloser {
				return xz.NewWriterLevel(w, level)
			},
		}, nil
	}
	return Spec{}, fmt.Errorf("unrecognized compression format %d", format)
}
