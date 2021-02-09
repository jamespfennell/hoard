package config

import "time"

type Feed struct {
	ID          string
	UserPrefix  *string `json:"prefix"`
	Postfix     string
	URL         string
	Periodicity time.Duration
	Variation   time.Duration
	Headers     map[string]string
}

func (f *Feed) Prefix() string {
	if f.UserPrefix != nil {
		return *f.UserPrefix
	}
	return f.ID + "_"
}

type Config struct {
	ArchivesPerHour int
	UploadsPerHour  int
	Port            int
	WorkspacePath   string

	Feeds         []Feed
	ObjectStorage []struct {
		ID string
	}
}

func NewDefaultConfig() Config {
	return Config{
		ArchivesPerHour: 1,
		UploadsPerHour:  1,
		Port:            8080,
		WorkspacePath:   "workspace",
	}
}

// NewConfigFromURL builds a Config object from a remote YAML file.
func NewConfigFromURL(url string) (Config, error) {
	return Config{}, nil
}
