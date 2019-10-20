package db

import (
	"errors"
	"fmt"
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

	flushChan chan []*KVPair

	lsm  *LSM
	vlog *VLog

	totalLsmReadDuration  time.Duration
	totalVlogReadDuration time.Duration
}

// NewDB creates a new database by instantiating the LSM and Value Log
func NewDB() (*DB, error) {
	lsm, err := NewLSM()
	if err != nil {
		return nil, err
	}

	vlog, err := NewVLog()
	if err != nil {
		return nil, err
	}

	db := &DB{
		mutable:   NewAVLTree(),
		immutable: NewAVLTree(),

		flushChan: make(chan []*KVPair),

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

	if db.mutable.size+13+len(key) > db.mutable.capacity {
		db.immutable = db.mutable
		db.mutable = NewAVLTree()
		pairs := db.immutable.Inorder()
		db.flushChan <- pairs
	}

	err := db.mutable.Put(key, value)
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
			return "", errors.New("Key not found")
		}
		return node.value, nil
	}

	if err != nil && err.Error() != "Key not found" {
		return "", err
	}

	node, err = db.immutable.Find(key)
	if node != nil {
		if node.value == "__delete__" {
			return "", errors.New("Key not found")
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
		return "", errors.New("Key not found")
	}

	return result.value, nil
}

// Delete deletes a given key from the database
func (db *DB) Delete(key string) error {
	return db.Put(key, "__delete__")
}

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

	result := []*KVPair{}

	pairs := db.mutable.Range(startKey, endKey)
	result = append(result, pairs...)

	pairs = db.immutable.Range(startKey, endKey)
	result = append(result, pairs...)

	sort.SliceStable(result, func(i, j int) bool {
		return result[i].key < result[j].key
	})

	return result, nil
}

func (db *DB) flush(KVPairs []*KVPair) error {
	db.immLock.Lock()
	defer db.immLock.Unlock()

	startKey := KVPairs[0].key
	endKey := KVPairs[len(KVPairs)-1].key

	vlogEntries := []byte{}

	lsmBlocks := []byte{}
	lsmIndex := []byte{}

	vlogOffset := db.vlog.head

	lsmBlock := []byte{}
	currBlock := uint32(0)

	for _, req := range KVPairs {
		// Create vlog entry
		vlogEntry, err := createVlogEntry(req.key, req.value)
		if err != nil {
			return err
		}
		vlogEntries = append(vlogEntries, vlogEntry...)

		// Create new block if current KVPair overflows block
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

		// Create lsmIndex entry if block is empty
		if len(lsmBlock) == 0 {
			indexEntry := createLsmIndex(req.key, currBlock)
			lsmIndex = append(lsmIndex, indexEntry...)
		}

		// Add lsmEntry to current block
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
	err := db.vlog.Append(vlogEntries)
	if err != nil {
		return err
	}

	// Append to lsm
	err = db.lsm.Append(lsmBlocks, lsmIndex, startKey, endKey)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) run() {
	for {
		select {
		case KVPairs := <-db.flushChan:
			err := db.flush(KVPairs)
			if err != nil {
				fmt.Printf("error flushing data from immutable table: %v\n", err)
			}
		}
	}
}
