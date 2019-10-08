package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const blockSize = 64
const keySize = 255
const valueSize = 65535

type lsmEntry struct {
	key    string
	offset int
}

type LSM struct {
	mutable   *AVLTree
	immutable *AVLTree
	immLock   sync.Mutex

	ssTable *SSTable

	flushChan chan struct{}

	vLog *VLog
}

func NewLSM() (*LSM, error) {
	vLog, err := NewVLog("vlog.log")
	if err != nil {
		return nil, err
	}
	ssTable, err := NewSSTable()
	if err != nil {
		return nil, err
	}

	lsm := &LSM{
		mutable:   NewAVLTree(),
		immutable: NewAVLTree(),
		flushChan: make(chan struct{}),
		ssTable:   ssTable,
		vLog:      vLog,
	}

	go lsm.run()

	return lsm, nil
}

func (lsm *LSM) Put(key, value string) error {
	// Data layout: (keySize uint8, valueSize uint16, key []byte (max 255 bytes), value []byte (max 64kb))
	if len(key) > keySize {
		return errors.New("Key size has exceeded 255 bytes")
	}
	if len(value) > valueSize {
		return errors.New("Value size has exceeded 64kb")
	}

	if lsm.mutable.size+13+len(key) > lsm.mutable.capacity {
		fmt.Printf("Put: %s, Flushing...\n", key)
		lsm.mutable, lsm.immutable = lsm.immutable, lsm.mutable
		lsm.flushChan <- struct{}{}
	}

	err := lsm.mutable.Put(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (lsm *LSM) Get(key string) (string, error) {
	if len(key) > keySize {
		return "", errors.New("Key size has exceeded 255 bytes")
	}

	node, err := lsm.mutable.Find(key)
	if node != nil {
		return node.value, nil
	}

	if err != nil && err.Error() != "Key not found" {
		return "", err
	}

	node, err = lsm.immutable.Find(key)
	if node != nil {
		return node.value, nil
	}

	if err != nil && err.Error() != "Key not found" {
		return "", err
	}

	offset, size, err := lsm.ssTable.Find(key)
	if err != nil {
		return "", err
	}

	result, err := lsm.vLog.Read(offset, size)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

func (lsm *LSM) Flush() error {
	lsm.immLock.Lock()
	defer lsm.immLock.Unlock()

	kvPairs := lsm.immutable.Inorder()

	vlogEntries := []byte{}
	lsmEntries := []byte{}

	vlogOffset := lsm.vLog.head

	for _, req := range kvPairs {
		keySize := uint8(len(req.key))
		valueSize := uint16(len(req.value))
		valueSizeBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(valueSizeBytes, valueSize)

		dataSize := 3 + len(req.key) + len(req.value)
		data := make([]byte, dataSize)

		i := 0
		i += copy(data[i:], []byte{keySize})
		i += copy(data[i:], req.key)
		i += copy(data[i:], valueSizeBytes)
		i += copy(data[i:], req.value)

		if i != dataSize {
			return errors.New("Expected length of data array does not match actual length")
		}

		vlogEntries = append(vlogEntries, data...)

		vlogOffset += i

		lsmEntry := make([]byte, 13+len(req.key))

		offsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBytes, uint64(vlogOffset))

		dataSizeBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(dataSizeBytes, uint32(dataSize))

		copy(lsmEntry[0:], []byte{keySize})
		copy(lsmEntry[1:], req.key)
		copy(lsmEntry[1+len(req.key):], offsetBytes)
		copy(lsmEntry[1+len(req.key)+8:], dataSizeBytes)

		lsmEntries = append(lsmEntries, lsmEntry...)
	}

	// Append to vlog
	err := lsm.vLog.Append(vlogEntries)
	if err != nil {
		return err
	}

	// Append to sstable
	err = lsm.ssTable.Append(lsmEntries)
	if err != nil {
		return err
	}

	lsm.mutable = NewAVLTree()

	return nil
}

func (lsm *LSM) run() {
	for {
		select {
		case <-lsm.flushChan:
			err := lsm.Flush()
			if err != nil {
				fmt.Printf("error flushing data from immutable table: %v\n", err)
			}
			fmt.Println("Done flushing")
		}
	}
}
