package db

import (
	"errors"
	"os"
)

// VLog is a struct that represents Value Log from WiscKey Paper
type VLog struct {
	fileName string
	tail     int
	head     int

	appendChan chan *appendRequest
}

type appendRequest struct {
	data      []byte
	replyChan chan error
}

// NewVLog creates vlog file and instantiates VLog struct
func NewVLog() (*VLog, error) {
	vLog := &VLog{
		fileName: "data/VLog/vlog.log",

		tail: 0,
		head: 0,

		appendChan: make(chan *appendRequest),
	}

	f, err := os.OpenFile(vLog.fileName, os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()

	info, err := os.Stat(vLog.fileName)
	if err != nil {
		return nil, err
	}
	vLog.head = int(info.Size())

	if err != nil {
		return nil, err
	}

	go vLog.run()

	return vLog, nil
}

func (vlog *VLog) run() {
	for {
		select {
		case req := <-vlog.appendChan:
			err := vlog.append(req.data)
			req.replyChan <- err
		}
	}
}

// Append sends an append request to VLog channel to guarantee non-concurrent writes to file
func (vlog *VLog) Append(data []byte) error {
	replyChan := make(chan error, 1)
	vlog.appendChan <- &appendRequest{
		data:      data,
		replyChan: replyChan,
	}
	err := <-replyChan
	if err != nil {
		return err
	}
	return nil
}

func (vlog *VLog) append(data []byte) error {
	f, err := os.OpenFile(vlog.fileName, os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return errors.New("Num bytes written to VLog does not match size of data")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	vlog.head += numBytes

	return nil
}

// Get gets a KVPair from the vlog
func (vlog *VLog) Get(query *LSMFind) (*KVPair, error) {
	f, err := os.OpenFile(vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	data := make([]byte, query.size)

	bytesRead, err := f.ReadAt(data, int64(query.offset))
	if err != nil {
		return nil, err
	}

	if bytesRead != int(query.size) {
		return nil, errors.New("Did not read correct amount of bytes")
	}

	keySize := uint8(data[0])
	key := data[1 : 1+keySize]
	value := data[3+keySize:]
	return &KVPair{key: string(key), value: string(value)}, nil
}
