package integrationtests

import (
	"flag"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"gopkg.in/yaml.v2"
	"os"
	"os/exec"
	"strings"
	"time"
)

var hoardCmd = flag.String("hoard-cmd", "", "Usage TODO")

func writeConfigToTempFile(c *config.Config) (string, error) {
	b, err := yaml.Marshal(c)
	if err != nil {
		return "", err
	}
	// TODO: use the package constant instead of /tmp/hoard_tests
	f, err := os.CreateTemp("/tmp/hoard_tests", "hoard-config-*.yml")
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return "", err
	}
	_, err = f.Write(b)
	if err != nil {
		_ = f.Close()
		return "", err
	}
	return f.Name(), f.Close()
}

func Execute(action Action, c *config.Config) error {
	if *hoardCmd == "" {
		return action.PackageCmd()(c)
	}
	configPath, err := writeConfigToTempFile(c)
	defer os.Remove(configPath)
	if err != nil {
		return err
	}
	args := append(
		append(
			strings.Fields(*hoardCmd),
			"--config-file", configPath,
		),
		action.CLIArgs()...,
	)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = "/home/james/git/hoard" // TODO: customize
	stdout, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error running command `%s`\n%s\n",
			cmd.String(), err)
		fmt.Println("Stdout:")
		fmt.Println(string(stdout))
	}
	return err
}

type Action interface {
	PackageCmd() func(c *config.Config) error
	CLIArgs() []string
}

type BasicAction int

const (
	Download BasicAction = iota
	Pack
	Merge
	Upload
)

func (action BasicAction) PackageCmd() func(c *config.Config) error {
	switch action {
	case Download:
		return hoard.Download
	case Pack:
		return hoard.Pack
	case Merge:
		return hoard.Merge
	case Upload:
		return hoard.Upload
	}
	panic("unknown command")
}

func (action BasicAction) CLIArgs() []string {
	switch action {
	case Download:
		return []string{"download"}
	case Pack:
		return []string{"pack"}
	case Merge:
		return []string{"merge"}
	case Upload:
		return []string{"upload"}
	}
	panic("unknown action")
}

// TODO: support running the integration tests through the CLI
//  in addition to the go package
// TODO: ExecuteUsingCLI()

type retrieve struct {
	Path string
}

func Retrieve(path string) Action {
	return retrieve{Path: path}
}

func (r retrieve) PackageCmd() func(*config.Config) error {
	return func(c *config.Config) error {
		return hoard.Retrieve(c, hoard.RetrieveOptions{
			Path:       r.Path,
			KeepPacked: false, // TODO?
			Start:      time.Now().Add(-60 * time.Minute).UTC(),
			// TODO: the api is bad here in the null case, should issue a warning
			// TODO: changing to UTC should not change the behavior
			End: time.Now().UTC(),
		})
	}
}

func (r retrieve) CLIArgs() []string {
	panic("undefined CLIArgs")
}
