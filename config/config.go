package config

import (
	_ "embed"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

//go:embed hoard.yml
var SampleConfig string

type Feed struct {
	ID          string
	UserPrefix  *string `yaml:"prefix,omitempty"`
	Postfix     string
	Periodicity time.Duration
	URL         string
	Headers     map[string]string
	Compression Compression
}

func (f *Feed) Prefix() string {
	if f.UserPrefix != nil {
		return *f.UserPrefix
	}
	return f.ID + "_"
}

type ObjectStorage struct {
	Endpoint   string
	AccessKey  string `yaml:"accessKey"`
	SecretKey  string `yaml:"secretKey"`
	BucketName string `yaml:"bucketName"`
	Prefix     string
	Insecure   bool
}

type Config struct {
	Port          int
	WorkspacePath string `yaml:"workspacePath"`

	Feeds          []Feed
	ObjectStorage  []ObjectStorage `yaml:"objectStorage"`
	Secrets        []string
	DisableMerging bool `yaml:"disableMerging"`
	PacksPerHour   int  `yaml:"packsPerHour"`
	UploadsPerHour int  `yaml:"uploadsPerHour"`
	Sync           bool
	LogLevel       string `yaml:"logLevel"`
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
		return nil, fmt.Errorf("failed to parse the config file as a YAML Hoard config: %w", err)
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

func (c *Config) LogLevelParsed() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(c.LogLevel)); err != nil {
		l = slog.LevelInfo
	}
	return l
}
