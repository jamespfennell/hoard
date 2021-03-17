package integrationtests

import (
	"fmt"
	"github.com/jamespfennell/hoard"
	"github.com/jamespfennell/hoard/config"
	"time"
)

type Action interface {
	ExecuteUsingPackage(c *config.Config) error
}

type BasicAction int

const (
	Download BasicAction = iota
	Pack
	Merge
	Upload
)

func (action BasicAction) ExecuteUsingPackage(c *config.Config) error {
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

// TODO: support running the integration tests through the CLI
//  in addition to the go package
// TODO: ExecuteUsingCLI()

type Retrieve struct {
	Path string
}

func (retrieve Retrieve) ExecuteUsingPackage(c *config.Config) error {
	return hoard.Retrieve(c, hoard.RetrieveOptions{
		Path:       retrieve.Path,
		KeepPacked: false, // TODO?
		Start:      time.Now().Add(-60 * time.Minute).UTC(),
		// TODO: the api is bad here in the null case, should issue a warning
		End: time.Now().UTC(),
	})
}
