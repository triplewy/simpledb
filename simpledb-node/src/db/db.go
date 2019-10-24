package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// DB is struct for database
type DB struct {
	mutable   *AVLTree
	immutable *AVLTree
	immLock   sync.Mutex

	flushChan chan []*LSMDataEntry

	lsm  *LSM
	vlog *VLog

	totalLsmReadDuration  time.Duration
	totalVlogReadDuration time.Duration
}

// NewDB creates a new database by instantiating the LSM and Value Log
func NewDB(directory string) (*DB, error) {
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, err
	}

	lsm, err := NewLSM(directory)
	if err != nil {
		return nil, err
	}

	vlog, err := NewVLog(directory)
	if err != nil {
		return nil, err
	}

	db := &DB{
		mutable:   NewAVLTree(),
		immutable: NewAVLTree(),

		flushChan: make(chan []*LSMDataEntry),

		lsm:  lsm,
		vlog: vlog,

		totalLsmReadDuration:  0 * time.Second,
		totalVlogReadDuration: 0 * time.Second,
	}

	go db.run()

	return db, nil
}

// Put inserts a key, value pair into the database
func (db *DB) Put(key, value string) error {
	if len(key) > keySize {
		return errors.New("Key size has exceeded " + strconv.Itoa(keySize) + " bytes")
	}
	if len(value) > valueSize {
		return errors.New("Value size has exceeded " + strconv.Itoa(valueSize) + " bytes")
	}

	entry, err := db.vlog.Append(key, value)
	if err != nil {
		return err
	}

	if db.mutable.size+13+len(key) > db.mutable.capacity {
		db.immutable = db.mutable
		db.mutable = NewAVLTree()
		pairs := db.immutable.Inorder()
		db.flushChan <- pairs
	}

	err = db.mutable.Put(key, value, entry)
	if err != nil {
		return err
	}

	return nil
}

// Get retrieves value for a given key or returns key not found
func (db *DB) Get(key string) (string, error) {
	if len(key) > keySize {
		return "", errors.New("Key size has exceeded maximum size")
	}

	node, err := db.mutable.Find(key)
	if node != nil {
		if node.value == "__delete__" {
			return "", newErrKeyNotFound()
		}
		return node.value, nil
	}

	if err != nil && err.Error() != "Key not found" {
		return "", err
	}

	node, err = db.immutable.Find(key)
	if node != nil {
		if node.value == "__delete__" {
			return "", newErrKeyNotFound()
		}
		return node.value, nil
	}

	if err != nil && err.Error() != "Key not found" {
		return "", err
	}

	startTime := time.Now()
	reply, err := db.lsm.Find(key, 0)
	if err != nil {
		return "", err
	}
	db.totalLsmReadDuration += time.Since(startTime)

	startTime = time.Now()
	result, err := db.vlog.Get(reply)
	if err != nil {
		return "", err
	}
	db.totalVlogReadDuration += time.Since(startTime)

	if result.value == "__delete__" {
		return "", newErrKeyNotFound()
	}

	return result.value, nil
}

// Delete deletes a given key from the database
func (db *DB) Delete(key string) error {
	return db.Put(key, "__delete__")
}

// Range finds all key, value pairs within the given range of keys
func (db *DB) Range(startKey, endKey string) ([]*KVPair, error) {
	if len(startKey) > keySize {
		return nil, errors.New("Start Key size has exceeded maximum size")
	}
	if len(endKey) > keySize {
		return nil, errors.New("End Key size has exceeded maximum size")
	}
	if startKey > endKey {
		return nil, errors.New("Start Key is greater than End Key")
	}

	all := []*KVPair{}

	pairs := db.mutable.Range(startKey, endKey)
	all = append(all, pairs...)

	pairs = db.immutable.Range(startKey, endKey)
	all = append(all, pairs...)

	startTime := time.Now()
	lsmFinds, err := db.lsm.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}
	db.totalLsmReadDuration += time.Since(startTime)

	startTime = time.Now()
	pairs, err = db.vlog.Range(lsmFinds)
	if err != nil {
		return nil, err
	}
	db.totalVlogReadDuration += time.Since(startTime)
	all = append(all, pairs...)

	resultMap := make(map[string]string)
	result := []*KVPair{}

	for _, kvPair := range all {
		if _, ok := resultMap[kvPair.key]; !ok {
			resultMap[kvPair.key] = kvPair.value
			result = append(result, kvPair)
		}
	}

	sort.SliceStable(result, func(i, j int) bool {
		return result[i].key < result[j].key
	})

	return result, nil
}

func (db *DB) flush(entries []*LSMDataEntry) error {
	db.immLock.Lock()
	defer db.immLock.Unlock()

	startKey := entries[0].key
	endKey := entries[len(entries)-1].key

	lsmBlocks := []byte{}
	lsmIndex := []byte{}

	lsmBlock := []byte{}
	currBlock := uint32(0)

	for _, entry := range entries {
		// Create new block if current entry overflows block
		if len(lsmBlock)+13+len(entry.key) > blockSize {
			filler := make([]byte, blockSize-len(lsmBlock))
			lsmBlock = append(lsmBlock, filler...)

			if len(lsmBlock) != blockSize {
				return newErrIncorrectBlockSize()
			}

			lsmBlocks = append(lsmBlocks, lsmBlock...)
			lsmBlock = []byte{}
			currBlock++
		}

		// Create lsmIndex entry if block is empty
		if len(lsmBlock) == 0 {
			indexEntry := createLsmIndex(entry.key, currBlock)
			lsmIndex = append(lsmIndex, indexEntry...)
		}

		// Add lsmEntry to current block
		lsmEntry := createLsmEntry(entry.key, entry.vlogOffset, entry.vlogSize)
		lsmBlock = append(lsmBlock, lsmEntry...)
	}

	if len(lsmBlock) > 0 {
		filler := make([]byte, blockSize-len(lsmBlock))
		lsmBlock = append(lsmBlock, filler...)

		if len(lsmBlock) != blockSize {
			return newErrIncorrectBlockSize()
		}

		lsmBlocks = append(lsmBlocks, lsmBlock...)
	}

	// Flush to LSM
	err := db.lsm.Append(lsmBlocks, lsmIndex, startKey, endKey)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) GC() error {
	f, err := os.OpenFile(db.vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	data := make([]byte, gcThreshold)

	numBytes, err := f.ReadAt(data, int64(db.vlog.tail))
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return errors.New("Num bytes read does not match expected length of data")
	}

	type gcStruct struct {
		offset uint64
		key    string
		value  string
	}

	gcStructs := []*gcStruct{}

	i := 0
	for i < len(data) {
		offset := db.vlog.tail + uint64(i)

		keySize := uint8(data[i])
		i++
		if i+int(keySize) > len(data) {
			break
		}
		key := string(data[i : i+int(keySize)])
		i += int(keySize)
		if i+2 > len(data) {
			break
		}
		valueSizeBytes := data[i : i+2]
		valueSize := binary.LittleEndian.Uint16(valueSizeBytes)
		i += 2
		if i+int(valueSize) > len(data) {
			break
		}
		value := string(data[i : i+int(valueSize)])
		i += int(valueSize)

		gcStructs = append(gcStructs, &gcStruct{
			offset: offset,
			key:    key,
			value:  value,
		})
	}

	for _, gcs := range gcStructs {
		entry, err := db.lsm.Find(gcs.key, 0)
		if err != nil {
			continue
		} else if entry.vlogOffset == gcs.offset {
			err := db.Put(gcs.key, gcs.value)
			if err != nil {
				return err
			}
		}
	}

	db.vlog.tail += uint64(i)
	err = db.Put("tail", strconv.Itoa(int(db.vlog.tail)+i))
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) run() {
	// gcTicker := time.NewTicker(1 * time.Second)

	for {
		select {
		case entries := <-db.flushChan:
			err := db.flush(entries)
			if err != nil {
				fmt.Printf("error flushing data from immutable table: %v\n", err)
			}
			// case <-gcTicker.C:
			// 	if db.vlog.tail+gcThreshold < db.vlog.head {
			// 		err := db.GC()
			// 		if err != nil {
			// 			fmt.Printf("error garbage collecting vlog: %v\n", err)
			// 		}
			// 	}
		}
	}
}
