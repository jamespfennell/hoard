package config

import (
	_ "embed"
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"strings"
	"time"
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
	PacksPerHour   int `yaml:"packsPerHour"`
	UploadsPerHour int `yaml:"uploadsPerHour"`
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

func (c *Config) LogLevelParsed() logrus.Level {
	l, err := logrus.ParseLevel(c.LogLevel)
	if err != nil {
		l = logrus.InfoLevel
	}
	return l
}
