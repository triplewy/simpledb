package db

// FileManager handles all write and read operations on files in LSM.
// Centralized file manager is required to prevent 'too many files open' error
type FileManager struct {
	fileWriteChan chan *fileWriteReq
	fileMmapChan  chan *fileMmapReq
	fileFindChan  chan *fileFindReq
	fileRangeChan chan *fileRangeReq
}

type fileWriteReq struct {
	filename string
	data     []byte
	errChan  chan error
}

type fileMmapReq struct {
	filename  string
	replyChan chan []*LSMDataEntry
	errChan   chan error
}

type fileFindReq struct {
	filename  string
	key       string
	replyChan chan *LSMDataEntry
	errChan   chan error
}

type fileRangeReq struct {
	filename  string
	keyRange  *KeyRange
	replyChan chan []*LSMDataEntry
	errChan   chan error
}

// NewFileManager creates a new file manager that spawns allotted file workers
func NewFileManager() *FileManager {
	fm := &FileManager{
		fileWriteChan: make(chan *fileWriteReq),
		fileMmapChan:  make(chan *fileMmapReq),
		fileFindChan:  make(chan *fileFindReq),
		fileRangeChan: make(chan *fileRangeReq),
	}
	for i := 0; i < numWorkers; i++ {
		go fm.spawnFileWorker()
	}
	return fm
}

// spawnFileWorker takes care of all open file operations. This prevents 'too many open files' error by
// restricting amount of workers who can open files
func (fm *FileManager) spawnFileWorker() {
	for {
		select {
		case req := <-fm.fileWriteChan:
			err := writeNewFile(req.filename, req.data)
			req.errChan <- err
		case req := <-fm.fileMmapChan:
			entries, err := mmap(req.filename)
			req.errChan <- err
			req.replyChan <- entries
		case req := <-fm.fileFindChan:
			entry, err := fileFind(req.filename, req.key)
			req.errChan <- err
			req.replyChan <- entry
		case req := <-fm.fileRangeChan:
			entries, err := fileRange(req.filename, req.keyRange)
			req.errChan <- err
			req.replyChan <- entries
		}
	}
}

// Write writes an arbitrary sized byte slice to a file
func (fm *FileManager) Write(filename string, data []byte) error {
	errChan := make(chan error, 1)
	req := &fileWriteReq{
		filename: filename,
		data:     data,
		errChan:  errChan,
	}
	fm.fileWriteChan <- req
	return <-errChan
}

// MMap reads a file's data block and converts it to a slice of LSMDataEntry
func (fm *FileManager) MMap(filename string) ([]*LSMDataEntry, error) {
	replyChan := make(chan []*LSMDataEntry, 1)
	errChan := make(chan error, 1)
	req := &fileMmapReq{
		filename:  filename,
		replyChan: replyChan,
		errChan:   errChan,
	}
	fm.fileMmapChan <- req
	return <-replyChan, <-errChan
}

// Find attempts to find a LSMDataEntry that matches the given key within the file
func (fm *FileManager) Find(filename, key string) (*LSMDataEntry, error) {
	replyChan := make(chan *LSMDataEntry, 1)
	errChan := make(chan error, 1)
	req := &fileFindReq{
		filename:  filename,
		key:       key,
		replyChan: replyChan,
		errChan:   errChan,
	}
	fm.fileFindChan <- req
	return <-replyChan, <-errChan
}

// Range returns all LSMDataEntry within in the specified key range within the file
func (fm *FileManager) Range(filename string, keyRange *KeyRange) ([]*LSMDataEntry, error) {
	replyChan := make(chan []*LSMDataEntry, 1)
	errChan := make(chan error, 1)
	req := &fileRangeReq{
		filename:  filename,
		keyRange:  keyRange,
		replyChan: replyChan,
		errChan:   errChan,
	}
	fm.fileRangeChan <- req
	return <-replyChan, <-errChan
}
