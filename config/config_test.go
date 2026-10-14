package config

import "testing"

func TestConfig_SampleConfigIsReadable(t *testing.T) {
	_, err := NewConfig([]byte(SampleConfig))
	if err != nil {
		t.Errorf("Sample config is not readable: %s\n", err)
	}
}

func TestConfig_CompressionLevelIsClamped(t *testing.T) {
	c, err := NewConfig([]byte("feeds:\n  - id: feed\n    compression:\n      format: gzip\n      level: 12\n"))
	if err != nil {
		t.Fatalf("Failed to read config: %s\n", err)
	}
	if level := c.Feeds[0].Compression.LevelActual(); level != 9 {
		t.Errorf("Compression level is %d; expected 9\n", level)
	}
}
