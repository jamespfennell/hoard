package config

import (
	_ "embed"
	"fmt"
	"gopkg.in/yaml.v2"
	"strings"
	"time"
)

//go:embed hoard.yml
var SampleConfig string

type Feed struct {
	ID          string
	UserPrefix  *string `yaml:"prefix"`
	Postfix     string
	URL         string
	Periodicity time.Duration
	Headers     map[string]string
}

func (f *Feed) Prefix() string {
	if f.UserPrefix != nil {
		return *f.UserPrefix
	}
	return f.ID + "_"
}

type ObjectStorage struct {
	Endpoint   string
	AccessKey  string
	SecretKey  string
	BucketName string
	Prefix     string
	Insecure   bool
}

type Config struct {
	Port          int
	WorkspacePath string

	Feeds          []Feed
	ObjectStorage  []ObjectStorage
	Secrets        []string
	PacksPerHour   int
	UploadsPerHour int
	Sync           bool
}

func NewConfigWithDefaults() *Config {
	return &Config{
		PacksPerHour:   1,
		UploadsPerHour: 1,
		Port:           8080,
		WorkspacePath:  "workspace",
	}
}

func NewConfig(b []byte) (*Config, error) {
	c := NewConfigWithDefaults()
	err := yaml.UnmarshalStrict(b, c)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the config file as a YAML Hoard config: %w\n", err)
	}
	return c, nil
}

func (c *Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return "Error while marshalling config to YAML."
	}
	s := string(b)
	for _, secret := range c.Secrets {
		n := 40
		s = strings.ReplaceAll(s, secret, "<span class=\"secret\">"+strings.Repeat("&nbsp;", n)+"</span>")
	}
	return s
}
