package db

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// VLog is a struct that represents Value Log from WiscKey Paper
type VLog struct {
	fileName string

	tail uint64
	head uint64

	appendChan chan *appendRequest

	openTime time.Duration
}

type appendRequest struct {
	key   string
	value string

	replyChan chan *LSMDataEntry
	errChan   chan error
}

// NewVLog creates vlog file and instantiates VLog struct
func NewVLog(directory string) (*VLog, error) {
	err := os.MkdirAll(filepath.Join(directory, "VLog"), os.ModePerm)
	if err != nil {
		return nil, err
	}

	vLog := &VLog{
		fileName: filepath.Join(directory, "VLog/vlog.log"),

		tail: 0,
		head: 0,

		appendChan: make(chan *appendRequest),

		openTime: 0 * time.Second,
	}

	f, err := os.OpenFile(vLog.fileName, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if !strings.HasSuffix(err.Error(), "file exists") {
			return nil, err
		}
	} else {
		f.Close()
	}

	info, err := os.Stat(vLog.fileName)
	if err != nil {
		return nil, err
	}
	vLog.head = uint64(info.Size())

	if err != nil {
		return nil, err
	}

	go vLog.run()

	return vLog, nil
}

// Append sends an append request to VLog channel to guarantee non-concurrent writes to file
func (vlog *VLog) Append(key, value string) (*LSMDataEntry, error) {
	replyChan := make(chan *LSMDataEntry, 1)
	errChan := make(chan error, 1)

	vlog.appendChan <- &appendRequest{
		key:   key,
		value: value,

		replyChan: replyChan,
		errChan:   errChan,
	}

	var entry *LSMDataEntry
	var err error

	select {
	case reply := <-replyChan:
		entry = reply
		return reply, nil
	case reply := <-errChan:
		err = reply
	}

	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (vlog *VLog) append(key, value string) (*LSMDataEntry, error) {
	startOpen := time.Now()
	f, err := os.OpenFile(vlog.fileName, os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()
	vlog.openTime += time.Since(startOpen)
	if err != nil {
		return nil, err
	}

	data, err := createVlogEntry(key, value)
	if err != nil {
		return nil, err
	}

	numBytes, err := f.Write(data)
	if err != nil {
		return nil, err
	}
	if numBytes != len(data) {
		return nil, newErrWriteUnexpectedBytes("vlog")
	}

	err = f.Sync()
	if err != nil {
		return nil, err
	}

	result := &LSMDataEntry{
		keySize:    uint8(len(key)),
		key:        key,
		vlogOffset: vlog.head,
		vlogSize:   uint32(numBytes),
	}

	vlog.head += uint64(numBytes)

	return result, nil
}

// Get gets a KVPair from the vlog
func (vlog *VLog) Get(query *LSMDataEntry) (*KVPair, error) {
	f, err := os.OpenFile(vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	data := make([]byte, query.vlogSize)

	bytesRead, err := f.ReadAt(data, int64(query.vlogOffset)-int64(vlog.tail))
	if err != nil {
		return nil, err
	}

	if bytesRead != int(query.vlogSize) {
		return nil, newErrReadUnexpectedBytes("vlog")
	}

	keySize := uint8(data[0])
	key := string(data[1 : 1+keySize])
	value := string(data[3+keySize:])
	return &KVPair{key: key, value: value}, nil
}

// Range takes a list of queries and reads the corresponding key-value pairs from the vlog
func (vlog *VLog) Range(queries []*LSMDataEntry) ([]*KVPair, error) {
	f, err := os.OpenFile(vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	replyChan := make(chan *KVPair)
	errChan := make(chan error)
	var wg sync.WaitGroup

	result := []*KVPair{}
	errs := []error{}

	wg.Add(len(queries))

	for _, query := range queries {
		go parallelRead(f, query.vlogOffset, query.vlogSize, replyChan, errChan)
	}

	go func() {
		for {
			select {
			case reply := <-replyChan:
				result = append(result, reply)
				wg.Done()
			case err := <-errChan:
				errs = append(errs, err)
				wg.Done()
			}
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return result, errs[0]
	}

	return result, nil
}

func parallelRead(f *os.File, offset uint64, size uint32, replyChan chan *KVPair, errChan chan error) {
	data := make([]byte, size)

	bytesRead, err := f.ReadAt(data, int64(offset))
	if err != nil {
		errChan <- err
		return
	}
	if bytesRead != int(size) {
		errChan <- newErrReadUnexpectedBytes("vlog")
		return
	}

	keySize := uint8(data[0])
	key := string(data[1 : 1+keySize])
	value := string(data[3+keySize:])

	replyChan <- &KVPair{key: key, value: value}
}

func (vlog *VLog) run() {
	for {
		select {
		case req := <-vlog.appendChan:
			result, err := vlog.append(req.key, req.value)
			if err != nil {
				req.errChan <- err
			} else {
				req.replyChan <- result
			}
		}
	}
}
