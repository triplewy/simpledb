package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

const blockSize = 32

const keySize = 19
const valueSize = 65535

const l0Size = 2 * 1024 / blockSize
const l1Size = 10 * 1024 / blockSize

const l0IndexSize = l0Size * (3 + keySize)
const l1IndexSize = l1Size * (3 + keySize)

const filenameLength = 8
const compactThreshold = 4

type lsmEntry struct {
	key    string
	offset int
}

type LSM struct {
	mutable   *AVLTree
	immutable *AVLTree
	immLock   sync.Mutex

	ssTable *SSTable

	flushChan chan []*kvPair

	vLog *VLog
}

func NewLSM() (*LSM, error) {
	vLog, err := NewVLog("data/VLog/vlog.log")
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
		flushChan: make(chan []*kvPair),
		ssTable:   ssTable,
		vLog:      vLog,
	}

	go lsm.run()

	return lsm, nil
}

func (lsm *LSM) Put(key, value string) error {
	// Data layout: (keySize uint8, valueSize uint16, key []byte (max 255 bytes), value []byte (max 64kb))
	if len(key) > keySize {
		return errors.New("Key size has exceeded " + strconv.Itoa(keySize) + " bytes")
	}
	if len(value) > valueSize {
		return errors.New("Value size has exceeded " + strconv.Itoa(valueSize) + " bytes")
	}

	if lsm.mutable.size+13+len(key) > lsm.mutable.capacity {
		lsm.immutable = lsm.mutable
		lsm.mutable = NewAVLTree()
		pairs := lsm.immutable.Inorder()
		lsm.flushChan <- pairs
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

	replies, err := lsm.ssTable.Find(key)
	if err != nil {
		return "", err
	}

	result, err := lsm.vLog.Read(replies)
	if err != nil {
		return "", err
	}

	return result[0].value, nil
}

func (lsm *LSM) Flush(kvPairs []*kvPair) error {
	lsm.immLock.Lock()
	defer lsm.immLock.Unlock()

	startKey := kvPairs[0].key
	endKey := kvPairs[len(kvPairs)-1].key

	vlogEntries := []byte{}

	lsmBlocks := []byte{}
	lsmIndex := []byte{}

	vlogOffset := lsm.vLog.head

	lsmBlock := []byte{}
	currBlock := uint16(0)

	for _, req := range kvPairs {
		vlogEntry, err := createVlogEntry(req.key, req.value)
		if err != nil {
			return err
		}
		vlogEntries = append(vlogEntries, vlogEntry...)

		if len(lsmBlock)+13+len(req.key) > blockSize {
			filler := make([]byte, blockSize-len(lsmBlock))
			lsmBlock = append(lsmBlock, filler...)

			if len(lsmBlock) != blockSize {
				return errors.New("LSM data block does not match block size")
			}

			lsmBlocks = append(lsmBlocks, lsmBlock...)
			lsmBlock = []byte{}
			currBlock++
		}

		if len(lsmBlock) == 0 {
			indexEntry := createLsmIndex(req.key, currBlock)
			lsmIndex = append(lsmIndex, indexEntry...)
		}

		lsmEntry := createLsmEntry(req.key, vlogOffset, len(vlogEntry))
		lsmBlock = append(lsmBlock, lsmEntry...)

		vlogOffset += len(vlogEntry)
	}

	if len(lsmBlock) > 0 {
		filler := make([]byte, blockSize-len(lsmBlock))
		lsmBlock = append(lsmBlock, filler...)

		if len(lsmBlock) != blockSize {
			return errors.New("LSM data block does not match block size")
		}

		lsmBlocks = append(lsmBlocks, lsmBlock...)
	}

	// Append to vlog
	err := lsm.vLog.Append(vlogEntries)
	if err != nil {
		return err
	}

	// Append to sstable
	err = lsm.ssTable.Append(lsmBlocks, lsmIndex, startKey, endKey)
	if err != nil {
		return err
	}

	return nil
}

func createVlogEntry(key, value string) ([]byte, error) {
	keySize := uint8(len(key))
	valueSize := uint16(len(value))

	valueSizeBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(valueSizeBytes, valueSize)

	dataSize := 3 + len(key) + len(value)
	data := make([]byte, dataSize)

	i := 0
	i += copy(data[i:], []byte{keySize})
	i += copy(data[i:], key)
	i += copy(data[i:], valueSizeBytes)
	i += copy(data[i:], value)

	if i != dataSize {
		return nil, errors.New("Expected length of data array does not match actual length")
	}
	return data, nil
}

func createLsmEntry(key string, offset, size int) []byte {
	lsmEntry := make([]byte, 13+len(key))

	offsetBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))

	dataSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataSizeBytes, uint32(size))

	copy(lsmEntry[0:], []byte{uint8(len(key))})
	copy(lsmEntry[1:], key)
	copy(lsmEntry[1+len(key):], offsetBytes)
	copy(lsmEntry[1+len(key)+8:], dataSizeBytes)

	return lsmEntry
}

func createLsmIndex(key string, block uint16) []byte {
	indexEntry := make([]byte, 3+len(key))

	blockBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(blockBytes, block)

	copy(indexEntry[0:], []byte{uint8(len(key))})
	copy(indexEntry[1:], key)
	copy(indexEntry[1+len(key):], blockBytes)

	return indexEntry
}

func (lsm *LSM) run() {
	for {
		select {
		case kvPairs := <-lsm.flushChan:
			err := lsm.Flush(kvPairs)
			if err != nil {
				fmt.Printf("error flushing data from immutable table: %v\n", err)
			}
			fmt.Println("Done flushing")
		}
	}
}
