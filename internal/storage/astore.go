package storage

import (
	"github.com/jamespfennell/hoard/internal/storage/util"
)

type AFile struct {
	Prefix  string
	Postfix string
	Time    util.Hour
	Hash    util.Hash
}

type AStore interface {
	// TODO
	// StoreAFile(aFile AFile, content []byte) error

	// ListNonEmptyHours() ([]time.Time, error)

	// ListInHour(hour time.Time) ([]DFile, error)
}
