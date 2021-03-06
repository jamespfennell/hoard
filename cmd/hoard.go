package main

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/util"
	"github.com/urfave/cli/v2"
	"os"
	"time"
)

const configFile = "config-file"
const endHour = "end-hour"
const feed = "feed"
const flattenFeeds = "flatten-feeds"
const flattenHours = "flatten-hours"
const fix = "fix"
const keepPacked = "keep-packed"
const noConcurrency = "no-concurrency"
const port = "port"
const removeWorkspace = "remove-workspace"
const startHour = "start-hour"

func main() {
	app := &cli.App{
		Name:        "Hoard",
		Usage:       "a distributed data feed collection system",
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
				Usage:       "don't run feed operations concurrently",
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
					_ = cfg
					return hoard.Audit(
						cfg, c.Timestamp(startHour), *c.Timestamp(endHour), c.Bool(fix))
				},
				Description: "",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        fix,
						Usage:       "fix problems found in the audit",
						Value:       false,
						DefaultText: "false",
					},
					&cli.TimestampFlag{
						Name:        startHour,
						Usage:       "the first hour in the audit",
						DefaultText: "no lower bound on the hours audited",
						Layout:      "2006-01-02-15",
					},
					&cli.TimestampFlag{
						Name:        endHour,
						Usage:       "the last hour in the audit",
						Value:       cli.NewTimestamp(time.Now().UTC()),
						DefaultText: "current time",
						Layout:      "2006-01-02-15",
					},
				},
			},
			{
				Name:      "retrieve",
				Usage:     "retrieve data from remote storage",
				ArgsUsage: "path",
				Action: func(c *cli.Context) error {
					cfg, err := configFromCliContext(c)
					if err != nil {
						fmt.Println(err)
						return err
					}
					if c.Args().Len() != 1 {
						return fmt.Errorf("expected exactly 1 argument (the path to retrieve to); recieved %d", c.Args().Len())
					}
					return hoard.Retrieve(cfg, hoard.RetrieveOptions{
						Path:            c.Args().First(),
						KeepPacked:      c.Bool(keepPacked),
						FlattenTimeDirs: c.Bool(flattenHours),
						FlattenFeedDirs: c.Bool(flattenFeeds),
						Start:           *c.Timestamp(startHour),
						End:             *c.Timestamp(endHour),
					})
				},
				Description: "",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  keepPacked,
						Usage: "don't unpack archives after retrieving",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  flattenFeeds,
						Usage: "place files from different feeds in the same directories",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  flattenHours,
						Usage: "place files from different hours in the same directories",
						Value: false,
					},
					&cli.TimestampFlag{
						Name:        startHour,
						Usage:       "the first hour to retrieve",
						DefaultText: "24 hours ago",
						Value:       cli.NewTimestamp(time.Now().UTC().Add(-24 * time.Hour)),
						Layout:      "2006-01-02-15",
					},
					&cli.TimestampFlag{
						Name:        endHour,
						Usage:       "the last hour to retrieve",
						Value:       cli.NewTimestamp(time.Now().UTC()),
						DefaultText: "current time",
						Layout:      "2006-01-02-15",
					},
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error:", err)
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
