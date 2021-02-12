package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"io"
	"sort"
	"strings"
	"time"
)

const manifestFileName = ".hoard_manifest.json"

type Archive struct {
	hour        storage.Hour
	hashToBytes map[storage.Hash][]byte
	dFiles      map[storage.DFile]bool
}

type LockedArchive struct {
	manifest     manifest
	hashToBytes  map[storage.Hash][]byte
	dFiles       map[storage.DFile]bool
	sortedDFiles []storage.DFile
}

type manifest struct {
	Hash            storage.Hash
	Hour            storage.Hour
	SourceArchives  []manifest
	SourceDownloads []storage.DFile
	// TODO: assembler = IP Address?
}

func NewArchiveForWriting(hour storage.Hour) *Archive {
	return &Archive{
		hour:        hour,
		hashToBytes: map[storage.Hash][]byte{},
		dFiles:      map[storage.DFile]bool{},
	}
}

func NewArchiveFromSerialization(b []byte) (*LockedArchive, error) {
	l := LockedArchive{
		hashToBytes: map[storage.Hash][]byte{},
		dFiles:      map[storage.DFile]bool{},
	}
	gzr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var buffer bytes.Buffer
		if _, err = buffer.ReadFrom(tr); err != nil {
			return nil, err
		}
		if header.Name == manifestFileName {
			if err = json.Unmarshal(buffer.Bytes(), &l.manifest); err != nil {
				// TODO: generate the default manifest instead
				// TODO: if no manifest file, generate the default manifest instead
				//  Can guess the hour from one of the DFiles
				//  If 0 DFiles, should be an error anyway
				return nil, err
			}
			continue
		}
		dFile, ok := storage.NewDFileFromString(header.Name)
		if !ok {
			return nil, fmt.Errorf("unrecognized file %s", header.Name)
		}
		l.dFiles[dFile] = true
		l.hashToBytes[dFile.Hash] = buffer.Bytes()
	}

	// TODO: iterate over the archives too
	for _, dFile := range l.manifest.SourceDownloads {
		// TODO: verify that the hash is there?
		l.dFiles[dFile] = true
		l.sortedDFiles = append(l.sortedDFiles, dFile)
	}
	sort.Sort(storage.DFileList(l.sortedDFiles))
	return &l, nil
}

func (a *Archive) Store(dFile storage.DFile, content []byte) error {
	a.hashToBytes[dFile.Hash] = content
	a.dFiles[dFile] = true
	return nil
}

func (a *Archive) Delete(d storage.DFile) error {
	return fmt.Errorf("cannot delete %s: archives do not support deletion", d)
}

func (a *Archive) AddSourceArchive(source *LockedArchive) {
	// TODO
}

func (a *Archive) Lock() *LockedArchive {
	var list storage.DFileList
	for dFile, _ := range a.dFiles {
		list = append(list, dFile)
	}
	sort.Sort(list)

	var hashBuilder strings.Builder
	for _, dFile := range list {
		hashBuilder.WriteString(dFile.String())
	}
	hash := storage.CalculateHash([]byte(hashBuilder.String()))
	m := manifest{
		Hash:            hash,
		Hour:            a.hour,
		SourceArchives:  nil,  // TODO: add in source archives
		SourceDownloads: list, // TODO: add in all DFiles not accounted by source archives
	}

	l := LockedArchive{
		manifest:     m,
		hashToBytes:  a.hashToBytes,
		dFiles:       a.dFiles,
		sortedDFiles: list,
	}

	// Erase the references to data in the original pack so that the locked pack
	// cannot be tampered with
	*a = *NewArchiveForWriting(a.hour)
	return &l
}

func (l *LockedArchive) Get(d storage.DFile) ([]byte, error) {
	if !l.dFiles[d] {
		return nil, fmt.Errorf("no such DFile %s", d)
	}
	b, _ := l.hashToBytes[d.Hash]
	return b, nil
}

func (l *LockedArchive) ListNonEmptyHours() ([]storage.Hour, error) {
	if len(l.dFiles) == 0 {
		return nil, nil
	}
	return []storage.Hour{l.manifest.Hour}, nil
}

func (l *LockedArchive) ListInHour(h storage.Hour) ([]storage.DFile, error) {
	if h != l.manifest.Hour {
		return nil, nil
	}
	b := make([]storage.DFile, len(l.sortedDFiles))
	for i, dFile := range l.sortedDFiles {
		b[i] = dFile
	}
	return b, nil
}

func (l *LockedArchive) Serialize() ([]byte, error) {
	var buffer bytes.Buffer
	gzw := gzip.NewWriter(&buffer)
	tw := tar.NewWriter(gzw)

	var lastHash storage.Hash
	for _, dFile := range l.sortedDFiles {
		if lastHash == dFile.Hash {
			continue
		}
		content := l.hashToBytes[dFile.Hash]
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

	b, _ := json.MarshalIndent(l.manifest, "", "  ")
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

func (l *LockedArchive) Hash() storage.Hash {
	return l.manifest.Hash
}
