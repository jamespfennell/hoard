package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/astore"
	"github.com/jamespfennell/hoard/internal/storage/dstore"
	"log"
	"time"
)

func PeriodicArchiver(feed *config.Feed, dstore dstore.DStore, astore astore.AStore, interruptChan <-chan struct{}) {
	log.Print("starting archiver", feed)
	// TODO: start at a given time
	timer := time.NewTicker(feed.Periodicity)
	for {
		select {
		case <-timer.C:
			Archive(feed, dstore, astore)
		case <-interruptChan:
			log.Print("Stopped feed archiving for", feed.ID)
			return
		}
	}
}

func Archive(f *config.Feed, d dstore.DStore, a astore.AStore) error {
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
//  And have the Archive satisfy the DStore interface
func archiveHour(f *config.Feed, d dstore.DStore, a astore.AStore, hour storage.Hour) error {
	fmt.Println("Archiving ", hour)
	dFiles, err := d.ListInHour(hour)
	if err != nil {
		return err
	}
	fmt.Println("Found", len(dFiles), " d files!")
	archive := ArchiveT{
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
		Prefix:  f.Prefix,
		Postfix: f.Postfix,
		Time:    hour,
		Hash:    archive.Hash(),
	}
	return a.Store(aFile, content)
}

// .manifest.json
type Manifest struct {
	Hash         storage.Hash
	Hour         storage.Hour
	SourceAFiles []Manifest
	SourceDFiles []storage.DFile
}

type ArchiveT struct {
	hashToBytes map[storage.Hash][]byte

	// set of DFiles
	dFiles           map[storage.DFile]bool
	manifest         Manifest
	manifestUpToDate bool
}

func (a ArchiveT) Store(dFile storage.DFile, content []byte) error {
	a.hashToBytes[dFile.Hash] = content
	a.dFiles[dFile] = true
	a.manifestUpToDate = false
	return nil
}

func (a ArchiveT) Hash() storage.Hash {
	return a.manifest.Hash
}

func (a ArchiveT) Manifest() Manifest {
	if a.manifestUpToDate {
		return a.manifest
	}
	return a.manifest
}

// Returns an error if the manifest references DFiles that aren't in the archive
func (a ArchiveT) AddSourceManifest(m []Manifest) error {
	return nil
}

func (a ArchiveT) Serialize() ([]byte, error) {
	var buffer bytes.Buffer
	gzw := gzip.NewWriter(&buffer)
	tw := tar.NewWriter(gzw)
	for dFile, _ := range a.dFiles {
		content := a.hashToBytes[dFile.Hash]
		hdr := &tar.Header{
			Name:    storage.DFileToPersistenceKey(dFile).Name,
			Mode:    0600,
			Size:    int64(len(content)),
			ModTime: dFile.Time,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			// Skip or return?
			fmt.Println("Skipping...")
			continue
		}
		if _, err := tw.Write(content); err != nil {
			fmt.Println("Skipping...")
			// Skip or return?
			continue
		}
		fmt.Println("Written")
	}
	gzw.Close()
	// Sort the DFiles
	// Iterate over them, writing each into the archive
	//     with the exception that if the hash is the same as the previous skip it
	// Then generate the manifest
	// Serialize it into the archive
	return buffer.Bytes(), nil
}
