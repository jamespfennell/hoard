package storage

import (
	"testing"
	"time"
)

func TestDFile_StringRoundTrip(t *testing.T) {
	d := DFile{
		Prefix:  "a",
		Postfix: "b",
		Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
		Hash:    ExampleHash(),
	}

	d2, ok := NewDFileFromString(d.String())

	if !ok {
		t.Errorf("Expected %s could be converted to a DFile", d.String())
	}
	if d != d2 {
		t.Errorf("%v != %v", d, d2)
	}
}
