package compression

import (
	"fmt"
	"github.com/jamespfennell/xz"
	"gopkg.in/yaml.v2"
	"strings"
	"testing"
)

func TestSpec_UnmarshalYAML(t *testing.T) {
	cases := []struct {
		input          string
		expectedOutput Spec
		roundTrip      string
	}{
		{
			"format: xz",
			Spec{
				Format: Xz,
				Level:  xz.DefaultCompression,
			},
			"format: xz\nlevel: 6",
		},
		{
			"format: gzip\nlevel: 1",
			Spec{
				Format: Gzip,
				Level:  1,
			},
			"format: gzip\nlevel: 1",
		},
	}
	for i, testCase := range cases {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			actualOutput := Spec{}
			if err := yaml.Unmarshal([]byte(testCase.input), &actualOutput); err != nil {
				t.Fatalf("Unexpected error when unmarhsalling: %s", err)
			}
			if actualOutput != testCase.expectedOutput {
				t.Fatalf("Actual != expected. \n%#v != \n%#v", actualOutput.Format, testCase.expectedOutput.Format)
			}
			b, err := yaml.Marshal(actualOutput)
			if err != nil {
				t.Fatalf("Unexpected error when re-marhsalling: %s", err)
			}
			if strings.TrimSpace(string(b)) != testCase.roundTrip {
				t.Fatalf("YAML round trip not the identity: \n%q != \n%q",
					strings.TrimSpace(string(b)),
					testCase.roundTrip)
			}
		})
	}
}
