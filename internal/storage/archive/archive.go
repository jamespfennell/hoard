package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/util"
	"io"
	"strings"
	"time"
)

const ManifestFileName = ".hoard_manifest.json"

type Archive struct {
	hour            storage.Hour
	hashToBytes     map[storage.Hash][]byte
	dFiles          map[storage.DFile]bool
	sourceManifests []manifest
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
	Assembler       string
	AssemblyTime    time.Time
}

func (m *manifest) dFiles() map[storage.DFile]bool {
	result := map[storage.DFile]bool{}
	for _, sourceManifest := range m.SourceArchives {
		for dFile, _ := range sourceManifest.dFiles() {
			result[dFile] = true
		}
	}
	for _, sourceDownload := range m.SourceDownloads {
		result[sourceDownload] = true
	}
	return result
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
		if header.Name == ManifestFileName {
			if err = json.Unmarshal(buffer.Bytes(), &l.manifest); err != nil {
				fmt.Printf("The manifest is corrupted: %s; skipping\n", err)
				return nil, err
			}
			continue
		}
		dFile, ok := storage.NewDFileFromString(header.Name)
		if !ok {
			fmt.Printf("Unable to interpret DFile name %s; skipping\n", dFile)
			continue
		}
		l.dFiles[dFile] = true
		l.hashToBytes[dFile.Hash] = buffer.Bytes()
	}

	for dFile := range l.manifest.dFiles() {
		if _, ok := l.hashToBytes[dFile.Hash]; !ok {
			fmt.Printf("Found DFile %s whose contents were not in the archive\n; skipping", dFile)
			continue
		}
		l.dFiles[dFile] = true
		// Note this is not necessarily sorted right now but will be momentarily
		l.sortedDFiles = append(l.sortedDFiles, dFile)
	}
	storage.Sort(l.sortedDFiles)
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

func (a *Archive) AddSourceManifest(source *LockedArchive) error {
	a.sourceManifests = append(a.sourceManifests, source.manifest)
	return nil
}

func (a *Archive) Lock() *LockedArchive {
	id, ok := util.GetPublicIPAddress()
	if !ok {
		id = "unknown"
	}
	m := manifest{
		Hour:           a.hour,
		SourceArchives: a.sourceManifests,
		Assembler:      id,
		AssemblyTime:   time.Now().UTC(),
	}
	dFilesAccountedFor := m.dFiles()
	var dFilesNotFromArchives []storage.DFile
	var allDFiles []storage.DFile
	for dFile, _ := range a.dFiles {
		allDFiles = append(allDFiles, dFile)
		if dFilesAccountedFor[dFile] {
			continue
		}
		dFilesNotFromArchives = append(dFilesNotFromArchives, dFile)
	}
	storage.Sort(allDFiles)
	storage.Sort(dFilesNotFromArchives)

	var hashBuilder strings.Builder
	for _, dFile := range allDFiles {
		hashBuilder.WriteString(dFile.String())
	}
	m.Hash = storage.CalculateHash([]byte(hashBuilder.String()))
	m.SourceDownloads = dFilesNotFromArchives

	l := LockedArchive{
		manifest:     m,
		dFiles:       a.dFiles,
		sortedDFiles: allDFiles,
		hashToBytes:  a.hashToBytes,
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
		Name:    ManifestFileName,
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
