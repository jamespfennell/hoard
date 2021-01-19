package storage

import (
	"github.com/jamespfennell/hoard/internal/storage/util"
	"time"
)

type Hour time.Time

type AFile struct {
	Prefix  string
	Postfix string
	Time    Hour
	Hash    util.Hash
}

type AStore interface {
	// TODO
	// StoreAFile(aFile AFile, content []byte) error

	// ListNonEmptyHours() ([]time.Time, error)

	// ListDFilesForHour(hour time.Time) ([]DFile, error)
}
