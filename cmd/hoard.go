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
const sync = "sync"
const port = "port"
const removeWorkspace = "remove-workspace"
const startHour = "start-hour"

const descriptionMain = `
Hoard is an application for collecting data feeds over time.

The central component of Hoard is the collector process, which is run using the
collector command. This process collects data from the configured feeds and stores
the results in remote object storage. The data can then be retrieved on any
computer using the retrieve command.

Hoard runs with a configuration file that specifies the feeds to collect, the object
storage locations in which to store data, and some other settings. Use the config
command to see an example config file.

Website: https://github.com/jamespfennell/hoard
`
const descriptionCollector = `
The Hoard collector is a process that generally runs all the time, collecting data
from the configured feeds and periodically uploading data to the configured remote
object storage locations.

The collector can (and generally should) be run simultaneously on multiple machines.
This will enable the collection process to continue even if one machine becomes
unavailable (for example, if the machine is being rebooted to apply OS updates).

The collector process launches an HTTP server that exports Prometheus metrics.
`
const descriptionPack = `
The pack action takes all downloaded files and bundles them into compressed archive
files.
`
const descriptionMerge = `
The merge action finds compressed archive files for the same hour, and merges them 
into a single new archive file.
`
const descriptionUpload = `
The upload action finds compressed archive files in the local workspace and transfers
them to remote object storage. The local files will be deleted afterwards. This action
automatically merges multiple archives for the same hour if such archives exist in
remote object storage.
`
const descriptionVacate = `
Vacate is mainly used when a machine running Hoard is being decommissioned. It
transfers all local data (downloaded files and archive files) to remote object storage.
This action is equivalent to running pack, merge, and upload.
`
const descriptionAudit = `
Auditing looks for problems in the data in remote object storage and optionally fixes
them. Currently, an audit looks for the following problems:

* Archive files that are present in one object storage but not in another. Fixing this
  will transfer files between remote storage.
* Hours that have multiple archive files for the same feed. Fixing this involves
  merging the archives together.
`

func main() {
	app := &cli.App{
		Name:        "Hoard",
		Usage:       "distributed data feed collection",
		Description: descriptionMain,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        configFile,
				Usage:       "path to the Hoard config file",
				Value:       "hoard.yml",
				DefaultText: "hoard.yml",
			},
			&cli.BoolFlag{
				Name:        sync,
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
				Name:  "config",
				Usage: "print an example Hoard configuration file",
				Action: func(*cli.Context) error {
					fmt.Print(config.SampleConfig)
					return nil
				},
			},
			{
				Name:  "verify",
				Usage: "verify the provided Hoard config is valid",
				Action: newAction(func(c *config.Config) error {
					fmt.Println("Provided config is valid!")
					return nil
				}),
			},
			{
				Name:        "collector",
				Usage:       "run the Hoard collector",
				Description: descriptionCollector,
				Action: newAction(func(c *config.Config) error {
					return hoard.RunCollector(util.WithSystemInterrupt(context.Background()), c)
				}),
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:        port,
						Usage:       "port the collection HTTP server will listen on",
						DefaultText: "read from config file",
					},
				},
			},
			{
				Name:      "retrieve",
				Usage:     "retrieve data from remote object storage",
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
						Usage:       "the first hour to retrieve in the form YYYY-MM-DD-HH",
						DefaultText: "24 hours ago",
						Value:       cli.NewTimestamp(time.Now().UTC().Add(-24 * time.Hour)),
						Layout:      "2006-01-02-15",
					},
					&cli.TimestampFlag{
						Name:        endHour,
						Usage:       "the last hour to retrieve in the form YYYY-MM-DD-HH",
						Value:       cli.NewTimestamp(time.Now().UTC()),
						DefaultText: "current time",
						Layout:      "2006-01-02-15",
					},
				},
			},
			{
				Name:   "download",
				Usage:  "run one download cycle for each feed",
				Action: newAction(hoard.Download),
			},
			{
				Name:        "pack",
				Usage:       "run one pack cycle for each feed",
				Description: descriptionPack,
				Action:      newAction(hoard.Pack),
			},
			{
				Name:        "merge",
				Usage:       "run one merge cycle for each feed",
				Description: descriptionMerge,
				Action:      newAction(hoard.Merge),
			},
			{
				Name:        "upload",
				Usage:       "run one upload cycle for each feed",
				Description: descriptionUpload,
				Action:      newAction(hoard.Upload),
			},
			{
				Name:        "vacate",
				Usage:       "move all local files from disk to remote object storage",
				Description: descriptionVacate,
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
				Name:        "audit",
				Usage:       "perform an audit of the data stored remotely",
				Description: descriptionAudit,
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
	if c.IsSet(sync) {
		cfg.Sync = c.Bool(sync)
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
			return err
		}
		return f(cfg)
	}
}
