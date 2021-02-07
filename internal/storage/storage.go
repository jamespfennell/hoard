package storage

import (
	"time"
)

type DFile struct {
	Prefix  string
	Postfix string
	Time    time.Time
	Hash    Hash
}

// TODO: test this
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
	Prefix  string
	Postfix string
	Time    Hour
	Hash    Hash
}
