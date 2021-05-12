package config

import "testing"

func TestConfig_SampleConfigIsReadable(t *testing.T) {
	_, err := NewConfig([]byte(SampleConfig))
	if err != nil {
		t.Errorf("Sample config is not readable: %s\n", err)
	}
}
