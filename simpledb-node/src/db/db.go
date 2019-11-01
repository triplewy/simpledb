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
	errChan    chan error

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
	mutable, err := NewMemTable(lsm, directory, "1")
	if err != nil {
		return nil, err
	}
	immutable, err := NewMemTable(lsm, directory, "2")
	if err != nil {
		return nil, err
	}

	db := &DB{
		seqID: 0,

		mutable:   mutable,
		immutable: immutable,

		insertChan: make(chan *insertRequest),
		getChan:    make(chan *getRequest),
		errChan:    make(chan error),

		lsm: lsm,

		close: make(chan struct{}, 1),
	}

	go db.run()

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
func (db *DB) Get(key string) (interface{}, error) {
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

	entries, err := db.lsm.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}
	all = append(all, entries...)

	sort.SliceStable(all, func(i, j int) bool {
		return all[i].key < all[j].key
	})

	result := make([]*KV, len(all))
	for i, entry := range all {
		data, err := parseDataEntry(entry)
		if err != nil {
			return nil, err
		}
		result[i] = data
	}
	return result, nil
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
			entry, err := createDataEntry(db.seqID, req.key, req.value)
			if err != nil {
				req.errChan <- err
			} else {
				entrySize := sizeDataEntry(entry)
				if db.mutable.size+entrySize > MemTableSize {
					go db.mutable.Flush(db.errChan)
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
		case err := <-db.errChan:
			fmt.Println(err)
		case <-db.close:
			db.lsm.Close()
			return
		}
	}
}
