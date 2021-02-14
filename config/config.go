package config

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// TODO: rename source?
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
	Secrets []string
}

func NewDefaultConfig() Config {
	return Config{
		ArchivesPerHour: 1,
		UploadsPerHour:  1,
		Port:            8080,
		WorkspacePath:   "workspace",
	}
}

// TODO: implement this?
// NewConfigFromURL builds a Config object from a remote YAML file.
func NewConfigFromURL(url string) (Config, error) {
	return Config{}, nil
}

func (c *Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return "Error while marshalling config to YAML"
	}
	s := string(b)
	for _, secret := range c.Secrets {
		n := len(secret)
		s = strings.ReplaceAll(s, secret, "<span class=\"secret\">"+strings.Repeat("&nbsp;", n)+"</span>")
	}
	return s
}

type CliIntegrator struct {
	flagValues struct {
		configFile string
	}
}

func (i *CliIntegrator) Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "config_file",
			Usage:       "path to the Hoard config file",
			DefaultText: "hoard.yml",
			Destination: &i.flagValues.configFile,
		},
	}
	// TODO: support overriding the port, etc.
}

func (i *CliIntegrator) NewAction(f func(*Config) error) cli.ActionFunc {
	return func(*cli.Context) error {
		c, err := i.BuildConfig()
		if err != nil {
			return err
		}
		return f(c)
	}
}

func (i *CliIntegrator) BuildConfig() (*Config, error) {
	b, err := ioutil.ReadFile(i.flagValues.configFile)
	if err != nil {
		fmt.Printf("Failed to read the config file: %s\n", err)
		return nil, err
	}
	c2 := NewDefaultConfig()
	err = yaml.Unmarshal(b, &c2)
	if err != nil {
		fmt.Printf("Failed to parse the config file as a YAML Hoard config: %s\n", err)
		return nil, err
	}
	var feedIDs []string
	for _, feed := range c2.Feeds {
		feedIDs = append(feedIDs, feed.ID)
	}
	fmt.Printf("Using %d feeds: %s\n", len(feedIDs), strings.Join(feedIDs, ", "))
	return &c2, nil
}

func (i *CliIntegrator) NewSystemInterruptChannel() <-chan struct{} {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	interruptC := make(chan struct{})
	go func() {
		<-sigC
		close(interruptC)
	}()
	return interruptC
}
