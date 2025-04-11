package integrationtests

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"gopkg.in/yaml.v2"
)

var hoardCmd = flag.String(
	"hoard-cmd",
	"",
	"the terminal command used to invoke Hoard (e.g., 'go run cmd/hoard.go'). If omitted, tests use the Go API",
)
var hoardOptionalCleanUp = flag.Bool(
	"hoard-cleanup-optional",
	false,
	"If set, tests will not fail if temporary files created by the tests cannot be deleted",
)
var hoardTmpDir = flag.String(
	"hoard-tmp-dir",
	"/tmp/hoard_tests",
	"directory to use for temporary test files",
)
var hoardWorkingDir = flag.String(
	"hoard-working-dir", "",
	"the working directory when invoking commands (defaults to the directory containing the test file)",
)

type Task interface {
	PackageCmd() func(c *config.Config) error
	CLIArgs() []string
}

func Execute(task Task, c *config.Config) error {
	if *hoardCmd == "" {
		return task.PackageCmd()(c)
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
		task.CLIArgs()...,
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

func ExecuteMany(tasks []Task, c *config.Config) error {
	for _, task := range tasks {
		if err := Execute(task, c); err != nil {
			return err
		}
	}
	return nil
}

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

type BasicTask int

const (
	Download BasicTask = iota
	Pack
	Merge
	Upload
)

func (task BasicTask) PackageCmd() func(c *config.Config) error {
	switch task {
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

func (task BasicTask) CLIArgs() []string {
	switch task {
	case Download:
		return []string{"download"}
	case Pack:
		return []string{"pack"}
	case Merge:
		return []string{"merge"}
	case Upload:
		return []string{"upload"}
	}
	panic("unknown task")
}

type retrieve struct {
	Path string
}

func Retrieve(path string) Task {
	return retrieve{Path: path}
}

func (r retrieve) PackageCmd() func(*config.Config) error {
	return func(c *config.Config) error {
		return hoard.Retrieve(c, hoard.RetrieveOptions{
			Path:       r.Path,
			KeepPacked: false,
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
