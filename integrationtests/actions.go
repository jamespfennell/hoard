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
var hoardOptionalCleanUp = flag.Bool("hoard-cleanup-optional", false, "Usage TODO")
var hoardTmpDir = flag.String("hoard-tmp-dir", "/tmp/hoard_tests", "Usage TODO")
var hoardWorkingDir = flag.String("hoard-working-dir", "", "TODO")

func writeConfigToTempFile(c *config.Config) (string, error) {
	b, err := yaml.Marshal(c)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(*hoardTmpDir, "hoard-config-*.yml")
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
	if *hoardWorkingDir != "" {
		cmd.Dir = *hoardWorkingDir
	}
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
	return []string{
		"retrieve",
		"--start-hour",
		time.Now().Add(-60 * time.Minute).UTC().Format("2006-01-02-15"),
		"--end-hour",
		time.Now().UTC().Format("2006-01-02-15"),
		r.Path,
	}
}
