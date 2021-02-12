package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"log"
	"sort"
	"strings"
	"time"
)

const manifestFileName = ".hoard_manifest.json"

func PeriodicArchiver(feed *config.Feed, dstore dstore.DStore, astore astore.AStore, interruptChan <-chan struct{}) {
	log.Print("starting archiver", feed)
	// TODO: start at a given time
	timer := time.NewTicker(time.Minute * 15) //feed.Periodicity)
	for {
		select {
		case <-timer.C:
			CreateFromDStore(feed, dstore, astore)
		case <-interruptChan:
			log.Print("Stopped feed archiving for", feed.ID)
			return
		}
	}
}

func CreateFromDStore(f *config.Feed, d dstore.DStore, a astore.AStore) error {
	hours, err := d.ListNonEmptyHours()
	if err != nil {
		return err
	}
	for _, hour := range hours {
		// TODO: archive the hours in parallel and use an error group
		archiveHour(f, d, a, hour)
	}
	return nil
}

// TODO: we can use this to combine archives too!
//  We just need a DStore that is based on multiple underlying DStores
//  And have the CreateFromDStore satisfy the DStore interface
func archiveHour(f *config.Feed, d dstore.DStore, a astore.AStore, hour storage.Hour) error {
	fmt.Println("Archiving ", hour)
	dFiles, err := d.ListInHour(hour)
	if err != nil {
		return err
	}
	if len(dFiles) == 0 {
		fmt.Println("Got no files!!")
		return nil
	}
	fmt.Println("Found", len(dFiles), " d files!")
	archive := Archive{
		hashToBytes: map[storage.Hash][]byte{},
		dFiles:      map[storage.DFile]bool{},
	}
	for _, dFile := range dFiles {
		content, err := d.Get(dFile)
		if err != nil {
			fmt.Println(err)
			// TODO: log the error
			continue
		}
		err = archive.Store(dFile, content)
		if err != nil {
			// TODO: log the error
		}
		fmt.Println("Added DFile")
	}
	// TODO: only continue if more than 1 dFile was written?
	content, err := archive.Serialize()
	if err != nil {
		return err
	}
	aFile := storage.AFile{
		Prefix:  f.Prefix(),
		Postfix: f.Postfix,
		Time:    hour,
		Hash:    archive.Hash(),
	}

	if err := a.Store(aFile, content); err != nil {
		return err
	}

	for _, dFile := range dFiles {
		if err := d.Delete(dFile); err != nil {
			fmt.Println("Error", err)
		}
	}
	return nil
}

// .manifest.json
type Manifest struct {
	Hash         storage.Hash
	Hour         storage.Hour
	SourceAFiles []Manifest
	SourceDFiles []storage.DFile

	// TODO: assembler = IP Address?
}

type Archive struct {
	hashToBytes map[storage.Hash][]byte
	dFiles      map[storage.DFile]bool

	upToDate     bool
	manifest     Manifest
	sortedDFiles []storage.DFile
}

func (a *Archive) Store(dFile storage.DFile, content []byte) error {
	a.hashToBytes[dFile.Hash] = content
	a.dFiles[dFile] = true
	a.upToDate = false
	return nil
}

func (a *Archive) Hash() storage.Hash {
	return a.manifest.Hash
}

func (a *Archive) Manifest() Manifest {
	a.refresh()
	return a.manifest
}

// Returns an error if the manifest references DFiles that aren't in the archive
func (a *Archive) AddSourceManifest(m Manifest) error {
	// TODO: validate the manifest -> ensure all referenced DFiles are in the archive
	// TODO: check it's no already added using its hash
	a.upToDate = false
	a.manifest.SourceAFiles = append(a.manifest.SourceAFiles, m)
	return nil
}

func (a *Archive) Serialize() ([]byte, error) {
	a.refresh()
	var buffer bytes.Buffer
	gzw := gzip.NewWriter(&buffer)
	tw := tar.NewWriter(gzw)
	var lastHash storage.Hash
	for _, dFile := range a.sortedDFiles {
		if lastHash == dFile.Hash {
			continue
		}
		content := a.hashToBytes[dFile.Hash]
		hdr := &tar.Header{
			Name:    dFile.String(),
			Mode:    0600,
			Size:    int64(len(content)),
			ModTime: dFile.Time,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write(content); err != nil {
			return nil, err
		}
		lastHash = dFile.Hash
	}

	// Write the Manifest
	fmt.Println("Writing the manifest..")
	a.manifest.Hour = storage.Hour(time.Now())
	a.manifest.SourceDFiles = a.sortedDFiles
	b, _ := json.MarshalIndent(a.manifest, "", "  ")
	hdr := &tar.Header{
		Name:    manifestFileName,
		Mode:    0600,
		Size:    int64(len(b)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}
	if _, err := tw.Write(b); err != nil {
		return nil, err
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gzw.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (a *Archive) refresh() {
	if a.upToDate {
		return
	}
	var l storage.DFileList
	for f, _ := range a.dFiles {
		l = append(l, f)
	}
	sort.Sort(l)
	a.sortedDFiles = l

	var hashBuilder strings.Builder
	for _, dFile := range a.sortedDFiles {
		hashBuilder.WriteString(string(dFile.Hash)) // TODO: need to include all of the dFile data here
	}
	// TODO: handle the error
	a.manifest.Hash, _ = storage.CalculateHash([]byte(hashBuilder.String()))

	a.upToDate = true

	// TODO: update the Hash and SourceDFiles
}
