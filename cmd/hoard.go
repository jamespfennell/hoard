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
const fix = "fix"
const feed = "feed"
const noConcurrency = "no_concurrency"
const port = "port"
const removeWorkspace = "remove_workspace"

func main() {
	app := &cli.App{
		Name:        "Hoard",
		Usage:       "a distributed data feed collection application",
		Description: "", // TODO and descriptions for all subcommands
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
			&cli.BoolFlag{
				Name:        noConcurrency,
				Usage:       "don't run feed option concurrently",
				DefaultText: "false",
			},
			&cli.StringSliceFlag{
				Name:    feed,
				Aliases: nil,
				Usage:   "if set, work will only be done for feeds with the specified IDs",
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
			{
				Name:  "vacate",
				Usage: "move all local files from disk to object storage",
				Action: func(c *cli.Context) error {
					cfg, err := configFromCliContext(c)
					if err != nil {
						fmt.Println(err)
						return err
					}
					return hoard.Vacate(cfg, c.Bool(removeWorkspace))
				},
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        removeWorkspace,
						Usage:       "remove workspace after vacating files",
						Value:       false,
						DefaultText: "false",
					},
				},
			},
			{
				Name:  "audit",
				Usage: "perform an audit of the data stored remotely",
				Action: func(c *cli.Context) error {
					cfg, err := configFromCliContext(c)
					if err != nil {
						fmt.Println(err)
						return err
					}
					return hoard.Audit(cfg, c.Bool(fix))
				},
				Description: "",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        fix,
						Usage:       "fix problems found in the audit",
						Value:       false,
						DefaultText: "false",
					},
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

func configFromCliContext(c *cli.Context) (*config.Config, error) {
	b, err := os.ReadFile(c.String(configFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read the Hoard config file: %w", err)
	}
	cfg, err := config.NewConfig(b)
	if err != nil {
		return nil, err
	}
	if c.IsSet(port) {
		cfg.Port = c.Int(port)
	}
	if c.IsSet(noConcurrency) {
		cfg.DisableConcurrency = c.Bool(noConcurrency)
	}
	if c.IsSet(feed) {
		feedIDs := c.StringSlice(feed)
		var feedsToKeep []config.Feed
		for _, feedID := range feedIDs {
			for _, feed := range cfg.Feeds {
				if feed.ID == feedID {
					feedsToKeep = append(feedsToKeep, feed)
				}
			}
		}
		cfg.Feeds = feedsToKeep
	}
	return cfg, nil
}

func newAction(f func(*config.Config) error) cli.ActionFunc {
	return func(c *cli.Context) error {
		cfg, err := configFromCliContext(c)
		if err != nil {
			fmt.Println(err)
			return err
		}
		return f(cfg)
	}
}
