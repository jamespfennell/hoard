package compression

import (
	"fmt"
	"github.com/jamespfennell/xz"
	"gopkg.in/yaml.v2"
	"testing"
)

func TestSpec_UnmarshalYAML(t *testing.T) {
	cases := []struct {
		input          string
		expectedOutput Spec
	}{
		{
			"format: xz",
			Spec{
				Format: Xz,
				Level:  xz.DefaultCompression,
			},
		},
		{
			"format: gzip\nlevel: 1",
			Spec{
				Format: Gzip,
				Level:  1,
			},
		},
	}
	for i, testCase := range cases {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			actualOutput := Spec{}
			if err := yaml.Unmarshal([]byte(testCase.input), &actualOutput); err != nil {
				t.Fatalf("Unexpected error when unmarhsalling: %s", err)
			}
			if actualOutput.Format != testCase.expectedOutput.Format {
				t.Fatalf("Actual != expected. \n%#v != \n%#v", actualOutput.Format, testCase.expectedOutput.Format)
			}
		})
	}
}
