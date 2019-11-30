package db

// fileManager handles all write and read operations on files in lsm.
// Centralized file manager is required to prevent 'too many files open' error
type fileManager struct {
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
	replyChan chan []*Entry
	errChan   chan error
}

type fileFindReq struct {
	filename  string
	key       string
	ts        uint64
	replyChan chan *Entry
	errChan   chan error
}

type fileRangeReq struct {
	filename  string
	keyRange  *keyRange
	ts        uint64
	replyChan chan []*Entry
	errChan   chan error
}

// newfileManager creates a new file manager that spawns allotted file workers
func newFileManager() *fileManager {
	fm := &fileManager{
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
func (fm *fileManager) spawnFileWorker() {
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
			entry, err := fileFind(req.filename, req.key, req.ts)
			req.errChan <- err
			req.replyChan <- entry
		case req := <-fm.fileRangeChan:
			entries, err := fileRange(req.filename, req.keyRange, req.ts)
			req.errChan <- err
			req.replyChan <- entries
		}
	}
}

// Write writes an arbitrary sized byte slice to a file
func (fm *fileManager) Write(filename string, data []byte) error {
	errChan := make(chan error, 1)
	req := &fileWriteReq{
		filename: filename,
		data:     data,
		errChan:  errChan,
	}
	fm.fileWriteChan <- req
	return <-errChan
}

// MMap reads a file's data block and converts it to a slice of lsmDataEntry
func (fm *fileManager) MMap(filename string) ([]*Entry, error) {
	replyChan := make(chan []*Entry, 1)
	errChan := make(chan error, 1)
	req := &fileMmapReq{
		filename:  filename,
		replyChan: replyChan,
		errChan:   errChan,
	}
	fm.fileMmapChan <- req
	return <-replyChan, <-errChan
}

// Find attempts to find a lsmDataEntry that matches the given key within the file
func (fm *fileManager) Find(filename, key string, ts uint64) (*Entry, error) {
	replyChan := make(chan *Entry, 1)
	errChan := make(chan error, 1)
	req := &fileFindReq{
		filename:  filename,
		key:       key,
		ts:        ts,
		replyChan: replyChan,
		errChan:   errChan,
	}
	fm.fileFindChan <- req
	return <-replyChan, <-errChan
}

// Range returns all lsmDataEntry within in the specified key range within the file
func (fm *fileManager) Range(filename string, keyRange *keyRange, ts uint64) ([]*Entry, error) {
	replyChan := make(chan []*Entry, 1)
	errChan := make(chan error, 1)
	req := &fileRangeReq{
		filename:  filename,
		keyRange:  keyRange,
		ts:        ts,
		replyChan: replyChan,
		errChan:   errChan,
	}
	fm.fileRangeChan <- req
	return <-replyChan, <-errChan
}
