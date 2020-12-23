package config

import "time"

type Feed struct {
	ID string
	Postfix string
	URL string
	Periodicity time.Duration
	Headers map[string]string
}

type Config struct {
	Feeds []Feed
	ObjectStorage []struct {
		ID string
	}
}

// NewConfigFromURL builds a Config object from a remote YAML file.
func NewConfigFromURL(url string) (Config, error) {
	return Config{}, nil
}