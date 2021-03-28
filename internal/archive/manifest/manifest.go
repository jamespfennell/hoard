package manifest

import (
	"encoding/json"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"github.com/jamespfennell/hoard/internal/util"
	"strings"
	"time"
)

// TODO: test serialization and deserialization?
func NewManifest(hr hour.Hour) *Manifest {
	return &Manifest{
		hour: hr,
		metadata: metadata{
			ipAddress: util.GetPublicIPAddressOr("<unknown>"),
			time:      time.Now().UTC(),
		},
		allDFiles: map[storage.DFile]bool{},
	}
}

func Deserialize(b []byte) (*Manifest, error) {
	var spec jsonSpec
	err := json.Unmarshal(b, &spec)
	if err != nil {
		return nil, err
	}
	return spec.toManifest(), nil
}

type Manifest struct {
	hour           hour.Hour
	hash           *storage.Hash
	metadata       metadata
	childManifests []Manifest
	originalDFiles []storage.DFile
	missingDFiles  []storage.DFile
	allDFiles      map[storage.DFile]bool
}

type metadata struct {
	ipAddress string
	time      time.Time
}

func (m *Manifest) DFiles() map[storage.DFile]bool {
	return m.allDFiles
}

func (m *Manifest) CalculateHash() storage.Hash {
	if m.hash == nil {
		dFiles := make([]storage.DFile, 0, len(m.allDFiles))
		for dFile := range m.allDFiles {
			dFiles = append(dFiles, dFile)
		}
		storage.Sort(dFiles)
		var hashBuilder strings.Builder
		for _, dFile := range dFiles {
			hashBuilder.WriteString(dFile.String())
		}
		h := storage.CalculateHash([]byte(hashBuilder.String()))
		m.hash = &h
	}
	return *m.hash
}

func (m *Manifest) Serialize() ([]byte, error) {
	return json.MarshalIndent(m.toJsonSpec(), "", "  ")
}

func (m *Manifest) AddOriginalDFiles(dFiles []storage.DFile) {
	m.originalDFiles = append(m.originalDFiles, dFiles...)
	for _, dFile := range m.originalDFiles {
		m.allDFiles[dFile] = true
	}
	m.hash = nil
}

func (m *Manifest) AddChildManifest(child *Manifest) {
	m.childManifests = append(m.childManifests, *child)
	for dFile := range child.DFiles() {
		m.allDFiles[dFile] = true
	}
	m.hash = nil
}

func (m *Manifest) MarkDFileMissing(dFile storage.DFile) {
	m.missingDFiles = append(m.missingDFiles, dFile)
	delete(m.allDFiles, dFile)
	m.hash = nil
}

func (m *Manifest) Hour() hour.Hour {
	return m.hour
}

func (m *Manifest) toJsonSpec() *jsonSpec {
	spec := jsonSpec{
		Hash:             m.CalculateHash(),
		Hour:             m.hour,
		Assembler:        m.metadata.ipAddress,
		AssemblyTime:     m.metadata.time,
		SourceDownloads:  m.originalDFiles,
		MissingDownloads: m.missingDFiles,
	}
	for _, child := range m.childManifests {
		spec.SourceArchives = append(spec.SourceArchives, *child.toJsonSpec())
	}
	return &spec
}

// TODO: add a version and then change the struct
type jsonSpec struct {
	Hash             storage.Hash
	Hour             hour.Hour
	Assembler        string
	AssemblyTime     time.Time
	SourceArchives   []jsonSpec
	SourceDownloads  []storage.DFile
	MissingDownloads []storage.DFile
}

func (j jsonSpec) toManifest() *Manifest {
	m := Manifest{
		hour: j.Hour,
		hash: &j.Hash,
		metadata: metadata{
			ipAddress: j.Assembler,
			time:      j.AssemblyTime,
		},
		allDFiles: map[storage.DFile]bool{},
	}
	for _, child := range j.SourceArchives {
		m.AddChildManifest(child.toManifest())
	}
	m.AddOriginalDFiles(j.SourceDownloads)
	for _, dFile := range j.MissingDownloads {
		m.MarkDFileMissing(dFile)
	}
	return &m
}
