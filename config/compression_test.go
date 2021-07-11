package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"regexp"
	"strings"
	"testing"
)

func Test_ExtensionRegex(t *testing.T) {
	var extensionMatcher = regexp.MustCompile(ExtensionRegex)
	for _, format := range allFormats {
		if extensionMatcher.FindStringSubmatch(format.Extension()) == nil {
			t.Errorf("Extension regex does not support format %v", format)
		}
	}
}

func TestSpec_UnmarshalYAML(t *testing.T) {
	cases := []struct {
		input          string
		expectedOutput Compression
		roundTrip      string
	}{
		{
			"",
			Compression{
				Format: Gzip,
			},
			"format: gzip",
		},
		{
			"format: xz",
			Compression{
				Format: Xz,
			},
			"format: xz",
		},
		{
			"format: gzip\nlevel: 1",
			NewSpecWithLevel(Gzip, 1),
			"format: gzip\nlevel: 1",
		},
	}
	for i, testCase := range cases {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			actualOutput := Compression{}
			if err := yaml.Unmarshal([]byte(testCase.input), &actualOutput); err != nil {
				t.Errorf("Unexpected error when unmarhsalling: %s", err)
			}
			if !actualOutput.Equal(testCase.expectedOutput) {
				t.Errorf("Actual != expected. \n%#v != \n%#v", actualOutput, testCase.expectedOutput)
			}
			b, err := yaml.Marshal(actualOutput)
			if err != nil {
				t.Errorf("Unexpected error when re-marhsalling: %s", err)
			}
			if strings.TrimSpace(string(b)) != testCase.roundTrip {
				t.Errorf("YAML round trip not the identity: \n%q != \n%q",
					strings.TrimSpace(string(b)),
					testCase.roundTrip)
			}
		})
	}
}
