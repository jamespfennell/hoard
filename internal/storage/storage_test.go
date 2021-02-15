package storage

import (
	"fmt"
	"testing"
	"time"
)

func TestDFile_StringRoundTrip(t *testing.T) {
	for i, d := range []DFile{
		{
			Prefix:  "a",
			Postfix: "b",
			Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
			Hash:    ExampleHash(),
		},
		{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2020, 1, 2, 3, 4, 5, 6*1000*1000, time.UTC),
			Hash:    ExampleHash(),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			d2, ok := NewDFileFromString(d.String())

			if !ok {
				t.Errorf("Expected %s could be converted to a DFile", d.String())
			}
			if d != d2 {
				t.Errorf("%v != %v", d, d2)
			}
		})
	}
}
