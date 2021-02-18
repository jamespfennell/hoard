package main

import (
	"context"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/util"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	var integrator config.CliIntegrator
	app := &cli.App{
		Flags: integrator.Flags(),
		Commands: []*cli.Command{
			{
				Name:  "collector",
				Usage: "runs the collection server",
				Action: integrator.NewAction(func(c *config.Config) error {
					return hoard.RunCollector(util.WithSystemInterrupt(context.Background()), c)
				}),
			},
			{
				Name:   "download",
				Usage:  "run one download cycle for each feed",
				Action: integrator.NewAction(hoard.Download),
			},
			{
				Name:   "pack",
				Usage:  "run one pack cycle for each feed",
				Action: integrator.NewAction(hoard.Pack),
			},
			{
				Name:   "merge",
				Usage:  "run one merge cycle for each feed",
				Action: integrator.NewAction(hoard.Merge),
			},
			{
				Name:   "upload",
				Usage:  "run one upload cycle for each feed",
				Action: integrator.NewAction(hoard.Upload),
			},
			// vacate --empty_trash
			// audit --dryrun
		},
	}
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
