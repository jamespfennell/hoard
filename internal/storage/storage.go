package storage

import (
	"errors"
	"fmt"
	"github.com/jamespfennell/hoard/config"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"
)

type Hash string

func ISO8601(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02dT%02d%02d%02dZ",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		// TODO: milliseconds
	)
}

type DFile struct {
	Feed *config.Feed
	Time time.Time
	Hash string
}

func (f *DFile) Path() string {
	// TODO: path package?
	return fmt.Sprintf("%s/%04d/%02d/%02d/%02d/%s_%s_%s%s",
		f.Feed.ID,
		f.Time.Year(),
		f.Time.Month(),
		f.Time.Day(),
		f.Time.Hour(),
		f.Feed.ID,
		ISO8601(f.Time),
		f.Hash,
		f.Feed.Postfix,
	)
}

type AStore interface {

}

type ADStore interface {
	AStore

	StoreDFile(dFile DFile, content []byte) error

	ListNonEmptyHours(feed *config.Feed) ([]time.Time, error)

	ListDFilesForHour(feed *config.Feed, hour time.Time) ([]DFile, error)
}

type Workspace struct {
	root string
}

func NewWorkspace(root string) Workspace {
	return Workspace{root: root}
}

func (w *Workspace) StoreDFile(dFile DFile, content []byte) error {
	fullPath := path.Join(w.root, "downloads", dFile.Path())
	err := os.MkdirAll(path.Dir(fullPath), os.ModePerm)
	if err != nil {
		return err
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	// TODO: handle the error
	defer f.Close()
	_, err = f.Write(content)
	return err
}

func (w *Workspace) ListNonEmptyHours(feed *config.Feed) ([]time.Time, error) {
	dirs, err := walkE(w.root)
	if err != nil {
		return nil, err
	}
	for _, dir := range dirs {
		if len(dir) != 4 {
			continue
		}
		ints, ok := cast(dir)
		if !ok {
			continue
		}
		// TODO: sanity check they can be converted, and then build the time
	}
}

func cast(input []string) ([]int, bool) {
	var output []int
	for _, s := range input {
		i, err := strconv.Atoi(s)
		if err != nil {
			return nil, false
		}
		output = append(output, i)
	}
	return output, true
}

type walkNode struct {
	file os.FileInfo
	next *walkNode
}

func walkE(root string) ([][]string, error) {
	var result [][]string
	nodes, err := walk(root)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		var thisResult []string
		for node != nil {
			thisResult = append(thisResult, node.file.Name())
			node = node.next
		}
		result = append(result, thisResult)
	}
	return result, nil
}

func walk(root string) ([]*walkNode, error) {
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var nodes []*walkNode
	for _, file := range files {
		nodes = append(nodes, &walkNode{file: file})
		if !file.IsDir() {
			continue
		}
		subNodes, err := walk(path.Join(root, file.Name()))
		if err != nil {
			return nil, err
		}
		for _, subNode := range subNodes {
			nodes = append(nodes, &walkNode{file: file, next: subNode})
		}
	}
	return nodes, nil
}

func getNumericDirectories(input []os.FileInfo) []string {
	var output []string
	for _, file := range input {
		if !file.IsDir() {
			continue
		}
		_, err := strconv.Atoi(file.Name())
		if err != nil {
			continue
		}
		output = append(output, file.Name())
	}
	return output
}

func (w *Workspace) ListDFilesForHour(feed *config.Feed, hour time.Time) ([]DFile, error) {
	return nil, nil
}