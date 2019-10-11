package db

import (
	"errors"
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

func (vlog *VLog) Read(queries []*LSMFind) ([]*kvPair, error) {
	f, err := os.OpenFile(vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	result := []*kvPair{}

	for _, item := range queries {
		data := make([]byte, item.size)

		bytesRead, err := f.ReadAt(data, int64(item.offset))
		if err != nil {
			return nil, err
		}

		if bytesRead != int(item.size) {
			return nil, errors.New("Did not read correct amount of bytes")
		}

		keySize := uint8(data[0])
		key := data[1 : 1+keySize]
		value := data[3+keySize:]
		result = append(result, &kvPair{key: string(key), value: string(value)})
	}

	return result, nil
}
