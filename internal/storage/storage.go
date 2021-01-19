package storage


/*
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

type Workspace struct {
	fs persistence.FileSystem
}

func NewWorkspace(fs persistence.FileSystem) Workspace {
	return Workspace{fs: fs}
}

func (w *Workspace) StoreDFile(dFile DFile, content []byte) error {
	fullPath := path.Join("downloads", dFile.Path())
	return w.fs.Put(fullPath, content)
}

func (w *Workspace) ListNonEmptyHours(feed *config.Feed) ([]time.Time, error) {
	/*
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
	return nil, nil
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
*/
