package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/hoard/internal/archive/manifest"
	"github.com/jamespfennell/hoard/internal/monitoring"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"io"
	"time"
)

const ManifestFileName = ".hoard_manifest.json"

func CreateFromDFiles(feed *config.Feed, dFiles []storage.DFile, dStore storage.ReadableDStore) (*Archive, error) {
	if len(dFiles) == 0 {
		return nil, fmt.Errorf("archive cannot contain zero downloaded files")
	}
	storage.Sort(dFiles)
	t := dFiles[0].Time
	m := manifest.NewManifest(hour.Date(t.Year(), t.Month(), t.Day(), t.Hour()))
	m.AddOriginalDFiles(dFiles)
	return createArchive(feed, *m, dStore), nil
}

func CreateFromAFiles(feed *config.Feed, aFiles []storage.AFile, aStore storage.ReadableAStore, tempDStore storage.DStore) (*Archive, error) {
	if len(aFiles) == 0 {
		return nil, fmt.Errorf("archive cannot contain zero downloaded files")
	}
	m := manifest.NewManifest(aFiles[0].Hour)
	dStore := hashBasedDStore{
		dStore: tempDStore,
		m:      map[storage.Hash]storage.DFile{},
	}
	var unpackedAFiles []storage.AFile
	var unpackedDFiles []storage.DFile
	for _, aFile := range aFiles {
		readerCloser, err := aStore.Get(aFile)
		if err != nil {
			continue
		}
		childM, dFiles, err := unpackInternal(readerCloser, dStore)
		if err != nil {
			_ = readerCloser.Close()
			continue
		}
		if readerCloser.Close() != nil {
			continue
		}
		unpackedDFiles = append(unpackedDFiles, dFiles...)
		unpackedAFiles = append(unpackedAFiles, aFile)
		m.AddChildManifest(childM)
	}
	// We now clean up the manifest so that the set of all files it references
	// is equal to the set of files inside the archive. First, we handle DFiles that
	// are referenced in the manifest but not in the archive.
	for dFile := range m.DFiles() {
		if !dStore.Contains(dFile) {
			m.MarkDFileMissing(dFile)
		}
	}
	// Second, we handle DFiles that are in the archive but not referenced in the
	// manifest
	var unaccountedForDFiles []storage.DFile
	for _, dFile := range unpackedDFiles {
		if !m.DFiles()[dFile] {
			unaccountedForDFiles = append(unaccountedForDFiles, dFile)
		}
	}
	m.AddOriginalDFiles(unaccountedForDFiles)

	a := createArchive(feed, *m, dStore)
	a.IncorporatedAFiles = unpackedAFiles
	return a, nil
}

func Unpack(content io.Reader, dStore storage.WritableDStore) error {
	_, _, err := unpackInternal(content, dStore)
	return err
}

func unpackInternal(content io.Reader, dStore storage.WritableDStore) (*manifest.Manifest, []storage.DFile, error) {
	var m *manifest.Manifest
	var dFiles []storage.DFile
	gzr, err := gzip.NewReader(content)
	if err != nil {
		return nil, nil, err
	}
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if header.Name == ManifestFileName {
			var buffer bytes.Buffer
			if _, err = buffer.ReadFrom(tr); err != nil {
				return nil, nil, err
			}
			if m, err = manifest.Deserialize(buffer.Bytes()); err != nil {
				fmt.Printf("The manifest is corrupted: %s; skipping\n", err)
				continue
			}
			continue
		}
		dFile, ok := storage.NewDFileFromString(header.Name)
		if !ok {
			fmt.Printf("Unable to interpret DFile name %s; skipping\n", dFile)
			_, _ = io.ReadAll(tr)
			continue
		}
		if err := dStore.Store(dFile, tr); err != nil {
			fmt.Printf("Error when storing DFile: %s", err)
			continue
		}
		dFiles = append(dFiles, dFile)
	}
	return m, dFiles, nil
}

func createArchive(feed *config.Feed, m manifest.Manifest, dStore storage.ReadableDStore) *Archive {
	dFiles := make([]storage.DFile, 0, len(m.DFiles()))
	for manifestDFile := range m.DFiles() {
		dFiles = append(dFiles, manifestDFile)
	}
	reader, writer := io.Pipe()
	a := &Archive{
		IncorporatedDFiles: dFiles,
		IncorporatedAFiles: nil,
		readCloser:         reader,
		feed:               feed,
		manifest:           m,
	}
	go a.write(writer, dStore)
	return a
}

type Archive struct {
	IncorporatedDFiles []storage.DFile
	IncorporatedAFiles []storage.AFile

	aFilesToDFileHashes map[storage.AFile][]storage.Hash
	readCloser          io.ReadCloser
	uncompressedSize    int
	feed                *config.Feed
	manifest            manifest.Manifest
}

func (archive *Archive) AFile() storage.AFile {
	return storage.AFile{
		Prefix: archive.feed.Prefix(),
		Hour:   archive.manifest.Hour(),
		Hash:   archive.manifest.CalculateHash(),
	}
}
func (archive *Archive) Reader() io.Reader {
	return archive.readCloser
}

func (archive *Archive) Close() error {
	return archive.readCloser.Close()
}

func (archive *Archive) write(writer *io.PipeWriter, dStore storage.ReadableDStore) {
	compressedBytesWriter := byteCounterWriter{Writer: writer}
	gzw := gzip.NewWriter(&compressedBytesWriter)
	uncompressedBytesWriter := byteCounterWriter{Writer: gzw}
	defer func() {
		_ = writer.CloseWithError(gzw.Close())
		monitoring.RecordPackSizes(archive.feed, uncompressedBytesWriter.BytesWritten, compressedBytesWriter.BytesWritten)
	}()
	tw := tar.NewWriter(&uncompressedBytesWriter)
	defer func() {
		if err := tw.Close(); err != nil {
			_ = writer.CloseWithError(err)
		}
	}()

	b, _ := archive.manifest.Serialize()
	if err := writeFileToArchive(tw, ManifestFileName, time.Now(), b); err != nil {
		_ = writer.CloseWithError(err)
		return
	}
	var lastHash storage.Hash
	dFiles := make([]storage.DFile, 0, len(archive.manifest.DFiles()))
	for dFile := range archive.manifest.DFiles() {
		dFiles = append(dFiles, dFile)
	}
	storage.Sort(dFiles)
	for _, dFile := range dFiles {
		if lastHash == dFile.Hash {
			continue
		}
		if err := writeDFileToArchive(tw, dFile, dStore); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		lastHash = dFile.Hash
	}
}

func writeDFileToArchive(tw *tar.Writer, dFile storage.DFile, dStore storage.ReadableDStore) error {
	content, err := dStore.Get(dFile)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(content)
	if err != nil {
		_ = content.Close()
		return err
	}
	if content.Close() != nil {
		return err
	}
	if err := writeFileToArchive(tw, dFile.String(), dFile.Time, b); err != nil {
		return err
	}
	return nil
}

func writeFileToArchive(tw *tar.Writer, fileName string, modTime time.Time, content []byte) error {
	hdr := &tar.Header{
		Name:    fileName,
		Mode:    0600,
		Size:    int64(len(content)),
		ModTime: modTime,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := tw.Write(content); err != nil {
		return err
	}
	return nil
}

// hashBasedDStore is a storage.DStore that essentially only uses a DFile's hash as a key when storing the file.
// Any DFile can be retrieved from the store as long as its hash is the same as a previous DFile that was stored.
// The motivation for this DStore is to undo the deduplication process that occurs when writing an archive file:
// the file that was not stored can be retrieved because its hash is equal to the file that was stored.
//
// For convenience, the implementation uses a backing DStore where the DFiles are ultimately saved. A map from hash
// to DFile allows the data structure to retrieve files by hash alone.
type hashBasedDStore struct {
	dStore storage.DStore
	m      map[storage.Hash]storage.DFile
}

func (dStore hashBasedDStore) Store(dFile storage.DFile, content io.Reader) error {
	if _, ok := dStore.m[dFile.Hash]; ok {
		return nil
	}
	err := dStore.dStore.Store(dFile, content)
	if err != nil {
		return err
	}
	dStore.m[dFile.Hash] = dFile
	return nil
}

func (dStore hashBasedDStore) Contains(dFile storage.DFile) bool {
	_, ok := dStore.m[dFile.Hash]
	return ok
}

func (dStore hashBasedDStore) Get(dFile storage.DFile) (io.ReadCloser, error) {
	backingDFile, ok := dStore.m[dFile.Hash]
	if !ok {
		return nil, fmt.Errorf("the DFile %s was not found", dFile)
	}
	return dStore.dStore.Get(backingDFile)
}

type byteCounterWriter struct {
	io.Writer
	BytesWritten int
}

func (b *byteCounterWriter) Write(p []byte) (n int, err error) {
	n, err = b.Writer.Write(p)
	b.BytesWritten++
	return
}
