package integrationtests

import (
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
)

type Action int

const (
	Download Action = iota
	Pack
	Merge
	Upload
)

func (action Action) ExecuteUsingPackage(c *config.Config) error {
	switch action {
	case Download:
		return hoard.Download(c)
	case Pack:
		return hoard.Pack(c)
	case Merge:
		return hoard.Merge(c)
	case Upload:
		return hoard.Upload(c)
	}
	return fmt.Errorf("unknown action")
}

// TODO: ExecuteUsingCLI()
