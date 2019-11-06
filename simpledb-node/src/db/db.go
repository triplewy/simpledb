package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// DB is struct for database
type DB struct {
	seqID uint64

	mutable   *MemTable
	immutable *MemTable

	insertChan chan *insertRequest
	getChan    chan *getRequest
	rangeChan  chan *rangeRequest
	flushChan  chan *MemTable

	lsm *LSM

	close chan struct{}
}

type insertRequest struct {
	key     string
	value   interface{}
	errChan chan error
}

type getRequest struct {
	key       string
	replyChan chan *LSMDataEntry
	errChan   chan error
}

type rangeRequest struct {
	startKey  string
	endKey    string
	replyChan chan []*LSMDataEntry
	errChan   chan error
}

type KV struct {
	key   string
	value interface{}
}

// NewDB creates a new database by instantiating the LSM and Value Log
func NewDB(directory string) (*DB, error) {
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(filepath.Join(directory, "metadata"), os.ModePerm)
	if err != nil {
		return nil, err
	}
	lsm, err := NewLSM(directory)
	if err != nil {
		return nil, err
	}
	memtable1, err := NewMemTable(directory, "1")
	if err != nil {
		return nil, err
	}
	memtable2, err := NewMemTable(directory, "2")
	if err != nil {
		return nil, err
	}

	db := &DB{
		seqID: 0,

		mutable:   memtable1,
		immutable: memtable2,

		insertChan: make(chan *insertRequest),
		getChan:    make(chan *getRequest),
		rangeChan:  make(chan *rangeRequest),
		flushChan:  make(chan *MemTable),

		lsm: lsm,

		close: make(chan struct{}, 1),
	}

	go db.run()
	go db.runFlush()

	return db, nil
}

// Put inserts a key, value pair into the database
func (db *DB) Put(key string, value interface{}) error {
	errChan := make(chan error, 1)

	req := &insertRequest{
		key:     key,
		value:   value,
		errChan: errChan,
	}

	db.insertChan <- req

	return <-errChan
}

// Get retrieves value for a given key or returns key not found
func (db *DB) Get(key string) (*KV, error) {
	if len(key) > KeySize {
		return nil, ErrExceedMaxKeySize(key)
	}

	entry := db.mutable.Get(key)
	if entry != nil {
		return parseDataEntry(entry)
	}

	entry = db.immutable.Get(key)
	if entry != nil {
		return parseDataEntry(entry)
	}

	replyChan := make(chan *LSMDataEntry, 1)
	errChan := make(chan error, 1)
	getRequest := &getRequest{
		key:       key,
		replyChan: replyChan,
		errChan:   errChan,
	}
	db.getChan <- getRequest

	select {
	case entry := <-replyChan:
		return parseDataEntry(entry)
	case err := <-errChan:
		return nil, err
	}
}

// Delete deletes a given key from the database
func (db *DB) Delete(key string) error {
	errChan := make(chan error, 1)

	req := &insertRequest{
		key:     key,
		value:   nil,
		errChan: errChan,
	}

	db.insertChan <- req

	return <-errChan
}

// Range finds all key, value pairs within the given range of keys
func (db *DB) Range(startKey, endKey string) ([]*KV, error) {
	if len(startKey) > KeySize {
		return nil, ErrExceedMaxKeySize(startKey)
	}
	if len(endKey) > KeySize {
		return nil, ErrExceedMaxKeySize(endKey)
	}
	if startKey > endKey {
		return nil, errors.New("Start Key is greater than End Key")
	}

	all := []*LSMDataEntry{}

	all = append(all, db.mutable.Range(startKey, endKey)...)
	all = append(all, db.immutable.Range(startKey, endKey)...)

	replyChan := make(chan []*LSMDataEntry, 1)
	errChan := make(chan error, 1)
	rangeRequest := &rangeRequest{
		startKey:  startKey,
		endKey:    endKey,
		replyChan: replyChan,
		errChan:   errChan,
	}
	db.rangeChan <- rangeRequest

	select {
	case entries := <-replyChan:
		all = append(all, entries...)
	case err := <-errChan:
		return nil, err
	}
	resultMap := make(map[string]*LSMDataEntry)

	for _, entry := range all {
		if value, ok := resultMap[entry.key]; !ok {
			resultMap[entry.key] = entry
		} else {
			if value.seqID < entry.seqID {
				resultMap[entry.key] = entry
			}
		}
	}

	result := []*KV{}
	for _, entry := range resultMap {
		data, err := parseDataEntry(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].key < result[j].key
	})

	return result, nil
}

// Flush takes all entries from the in-memory table and sends them to LSM
func (db *DB) Flush(mt *MemTable) error {
	entries := mt.table.Inorder()

	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		return err
	}
	// Flush to LSM
	err = db.lsm.Append(dataBlocks, indexBlock, bloom, keyRange)
	if err != nil {
		return err
	}
	// Truncate the WAL
	err = os.Truncate(mt.wal, 0)
	if err != nil {
		return err
	}

	mt.table = NewAVLTree()
	mt.size = 0
	return nil
}

// Close gracefully closes the database
func (db *DB) Close() {
	db.close <- struct{}{}
}

// ForceClose immediately shuts down the database. Good for testing
func (db *DB) ForceClose() {
	os.Exit(0)
}

func (db *DB) run() {
	for {
		select {
		case req := <-db.insertChan:
			db.seqID++
			entry, err := createDataEntry(db.seqID, req.key, req.value)
			if err != nil {
				req.errChan <- err
			} else {
				entrySize := sizeDataEntry(entry)
				if db.mutable.size+entrySize > MemTableSize {
					db.flushChan <- db.mutable
					db.mutable, db.immutable = db.immutable, db.mutable
				}
				err = db.mutable.Put(entry)
				if err != nil {
					req.errChan <- err
				} else {
					req.errChan <- nil
				}
			}
		case req := <-db.getChan:
			reply, err := db.lsm.Find(req.key, 0)
			if err != nil {
				req.errChan <- err
			} else {
				req.replyChan <- reply
			}
		case req := <-db.rangeChan:
			entries, err := db.lsm.Range(req.startKey, req.endKey)
			if err != nil {
				req.errChan <- err
			} else {
				req.replyChan <- entries
			}
		case <-db.close:
			db.lsm.Close()
			return
		}
	}
}

func (db *DB) runFlush() {
	for {
		select {
		case mt := <-db.flushChan:
			err := db.Flush(mt)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}
