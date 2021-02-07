package dstore

import (
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/persistence"
	"testing"
	"time"
)

func TestListNonEmptyHours(t *testing.T) {
	b := persistence.NewInMemoryBytesStorage()
	d := NewByteStorageBackedDStore(b)

	keys := []persistence.Key{
		{
			Prefix: []string{"2020", "02", "06", "17"},
			Name:   "c",
		},
		{
			Prefix: []string{"2020", "22", "06", "17"},
			Name:   "c",
		},
		{
			Prefix: []string{"a", "b", "c", "d"},
			Name:   "c",
		},
		{
			Prefix: []string{"2020", "02", "06"},
			Name:   "c",
		},
	}
	for _, key := range keys {
		if err := b.Put(key, []byte{1}); err != nil {
			t.Fatal("Failed to add key to byte storage:", key)
		}
	}

	hours, err := d.ListNonEmptyHours()
	if err != nil {
		t.Error("Unexpected error in ListNonEmptyHours:", err)
	}
	if len(hours) != 1 {
		t.Error("Unexpected hours:", hours)
	}
	expected := storage.Hour(time.Date(2020, 2, 6, 17, 0, 0, 0, time.UTC))
	if hours[0] != expected {
		t.Error("Unexpected hours:", hours, " expected:", expected)
	}
}
