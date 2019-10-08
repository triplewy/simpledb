package db

import (
	"errors"
	"fmt"
	"os"
)

type VLog struct {
	fileName string
	tail     int
	head     int

	appendChan chan *appendRequest
	closeChan  chan struct{}
}

type appendRequest struct {
	data      []byte
	replyChan chan error
}

func NewVLog(filename string) (*VLog, error) {
	vLog := &VLog{
		fileName:   filename,
		tail:       0,
		head:       0,
		appendChan: make(chan *appendRequest),
		closeChan:  make(chan struct{}, 1),
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
			numBytes, err := vlog.append(req.data)
			if numBytes != len(req.data) {
				req.replyChan <- errors.New("Num bytes written does not match size of data")
			} else {
				req.replyChan <- err
			}
		case <-vlog.closeChan:
			return
		}
	}
}

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

func (vlog *VLog) append(data []byte) (numBytes int, err error) {
	f, err := os.OpenFile(vlog.fileName, os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return 0, err
	}

	numBytes, err = f.Write(data)
	if err != nil {
		return 0, err
	}

	err = f.Sync()
	if err != nil {
		return 0, err
	}

	vlog.head += numBytes

	return numBytes, nil
}

func (vlog *VLog) Read(offset uint64, numBytes uint32) ([]byte, error) {
	f, err := os.OpenFile(vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	result := make([]byte, numBytes)

	bytesRead, err := f.ReadAt(result, int64(offset))
	if err != nil {
		return nil, err
	}

	if bytesRead != int(numBytes) {
		return nil, errors.New("Did not read correct amount of bytes")
	}

	fmt.Println("Vlog result:", result)
	keySize := uint8(result[0])
	value := result[3+keySize:]

	return value, nil
}
