package simpledb

import (
	"errors"
	"os"
)

type VLog struct {
	fileName string
	tail     int
	head     int
}

func NewVLog() (*VLog, error) {
	vLog := &VLog{
		fileName: "vlog.log",
		tail:     0,
		head:     0,
	}

	f, err := os.OpenFile(vLog.fileName, os.O_CREATE, 0644)
	defer f.Close()

	info, err := os.Stat(vLog.fileName)
	if err != nil {
		return nil, err
	}
	vLog.head = int(info.Size())

	if err != nil {
		return nil, err
	}
	return vLog, nil
}

func (vLog *VLog) append(value []byte) (numBytes, offset int, err error) {
	f, err := os.OpenFile(vLog.fileName, os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	numBytes, err = f.Write(value)
	if err != nil {
		return 0, 0, err
	}

	err = f.Sync()
	if err != nil {
		return 0, 0, err
	}

	offset = vLog.head
	vLog.head += numBytes
	return numBytes, offset, nil
}

func (vLog *VLog) read(offset, numBytes int) ([]byte, error) {
	f, err := os.OpenFile(vLog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	result := make([]byte, numBytes)
	bytesRead, err := f.ReadAt(result, int64(offset))
	if err != nil {
		return nil, err
	}
	if bytesRead != numBytes {
		return nil, errors.New("Did not read correct amount of bytes")
	}
	return result, nil
}
