package main

import (
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
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
					return hoard.RunCollector(c, integrator.NewSystemInterruptChannel())
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
			// merge
			// upload
			// vacate --empty_trash
		},
	}
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
