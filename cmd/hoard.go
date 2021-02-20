package main

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/util"
	"github.com/urfave/cli/v2"
	"os"
)

const configFile = "config_file"
const port = "port"

func main() {
	app := &cli.App{
		Name:        "Hoard",
		Usage:       "a distributed data feed collection application",
		Description: "", // TODO
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        configFile,
				Usage:       "path to the Hoard config file",
				Value:       "hoard.yml",
				DefaultText: "hoard.yml",
			},
			&cli.IntFlag{
				Name:        port,
				Usage:       "port the collection server will listen on",
				DefaultText: "read from config file",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "collector",
				Usage: "runs the collection server",
				Action: newAction(func(c *config.Config) error {
					return hoard.RunCollector(util.WithSystemInterrupt(context.Background()), c)
				}),
			},
			{
				Name:   "download",
				Usage:  "run one download cycle for each feed",
				Action: newAction(hoard.Download),
			},
			{
				Name:   "pack",
				Usage:  "run one pack cycle for each feed",
				Action: newAction(hoard.Pack),
			},
			{
				Name:   "merge",
				Usage:  "run one merge cycle for each feed",
				Action: newAction(hoard.Merge),
			},
			{
				Name:   "upload",
				Usage:  "run one upload cycle for each feed",
				Action: newAction(hoard.Upload),
			},
			// vacate --empty_trash
			// audit --dryrun
		},
	}
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

func newAction(f func(*config.Config) error) cli.ActionFunc {
	return func(c *cli.Context) error {
		b, err := os.ReadFile(c.String(configFile))
		// TODO: override port
		if err != nil {
			fmt.Println(
				fmt.Errorf("failed to read the Hoard config file: %w", err))
			return err
		}
		cfg, err := config.NewConfig(b)
		if err != nil {
			fmt.Println(err)
			return err
		}
		return f(cfg)
	}
}
