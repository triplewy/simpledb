package db

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

// DB is struct for database
type DB struct {
	oracle *oracle

	mutable   *memTable
	immutable *memTable

	batchChan chan *batchRequest
	flushChan chan *memTable

	lsm *lsm

	memory *memory

	close chan struct{}
}

type batchRequest struct {
	entries   []*kv
	replyChan chan error
}

// kv is struct for Key-Value pair
type kv struct {
	ts    uint64
	key   string
	value interface{}
}

// NewDB creates a new database by instantiating the lsm and Value Log
func NewDB(directory string) (*DB, error) {
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, err
	}
	lsm, err := newLSM(directory)
	if err != nil {
		return nil, err
	}
	memtable1, maxCommitTs1, err := newMemTable(directory, "1")
	if err != nil {
		return nil, err
	}
	memtable2, maxCommitTs2, err := newMemTable(directory, "2")
	if err != nil {
		return nil, err
	}

	maxCommitTs := maxCommitTs1
	if maxCommitTs2 > maxCommitTs {
		maxCommitTs = maxCommitTs2
	}

	db := &DB{
		mutable:   memtable1,
		immutable: memtable2,

		batchChan: make(chan *batchRequest),
		flushChan: make(chan *memTable),

		lsm: lsm,

		memory: newMemory(),

		close: make(chan struct{}, 1),
	}

	oracle := newOracle(maxCommitTs, db)
	db.oracle = oracle

	go db.run()
	go db.runFlush()

	return db, nil
}

// newTxn returns a new Txn to perform ops on
func (db *DB) newTxn() *Txn {
	startTs := db.oracle.requestStart()
	return &Txn{
		db:         db,
		startTs:    startTs,
		writeCache: make(map[string]*kv),
		readSet:    make(map[string]uint64),
	}
}

// ViewTxn implements a read only transaction to the DB. Ensures read only since it does not commit at end
func (db *DB) ViewTxn(fn func(txn *Txn) error) error {
	txn := db.newTxn()
	return fn(txn)
}

// UpdateTxn implements a read and write only transaction to the DB
func (db *DB) UpdateTxn(fn func(txn *Txn) error) error {
	txn := db.newTxn()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.commit()
}

// batchPut inserts multiple key-value pairs into DB
func (db *DB) batchPut(entries []*kv) error {
	replyChan := make(chan error, 1)
	req := &batchRequest{
		entries:   entries,
		replyChan: replyChan,
	}

	db.batchChan <- req
	return <-replyChan
}

// get retrieves value for a given key or returns key not found
func (db *DB) read(key string, ts uint64) (*kv, error) {
	if len(key) > KeySize {
		return nil, newErrExceedMaxKeySize(key)
	}
	entry := db.mutable.table.Find(key, ts)
	if entry != nil {
		return parseDataEntry(entry)
	}
	entry = db.immutable.table.Find(key, ts)
	if entry != nil {
		return parseDataEntry(entry)
	}
	entry, err := db.lsm.Read(key, ts)
	if err != nil {
		return nil, err
	}
	return parseDataEntry(entry)
}

// range finds all key, value pairs within the given range of keys
func (db *DB) scan(startKey, endKey string, ts uint64) ([]*kv, error) {
	if len(startKey) > KeySize {
		return nil, newErrExceedMaxKeySize(startKey)
	}
	if len(endKey) > KeySize {
		return nil, newErrExceedMaxKeySize(endKey)
	}
	if startKey > endKey {
		return nil, errors.New("Start Key is greater than End Key")
	}
	keyRange := &keyRange{startKey: startKey, endKey: endKey}

	all := []*lsmDataEntry{}
	all = append(all, db.mutable.table.Scan(keyRange, ts)...)
	all = append(all, db.immutable.table.Scan(keyRange, ts)...)

	entries, err := db.lsm.Scan(keyRange, ts)
	if err != nil {
		return nil, err
	}
	all = append(all, entries...)

	// Convert slice of entries into a set of entries
	resultMap := make(map[string]*lsmDataEntry)
	for _, entry := range all {
		if value, ok := resultMap[entry.key]; !ok {
			resultMap[entry.key] = entry
		} else {
			if value.ts < entry.ts {
				resultMap[entry.key] = entry
			}
		}
	}

	result := []*kv{}
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

// Flush takes all entries from the in-memory table and sends them to lsm
func (db *DB) flush(mt *memTable) error {
	entries := mt.table.Inorder()
	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		return err
	}
	// Flush to lsm
	err = db.lsm.Write(dataBlocks, indexBlock, bloom, keyRange)
	if err != nil {
		return err
	}
	// Truncate the WAL
	err = os.Truncate(mt.wal, 0)
	if err != nil {
		return err
	}
	mt.table = newAVLTree()
	mt.size = 0
	return nil
}

// Close gracefully closes the database
func (db *DB) Close() {
	db.close <- struct{}{}
}

// ForceClose immediately shuts down the database. Good for testing
func (db *DB) forceClose() {
	os.Exit(0)
}

func (db *DB) run() {
	for {
	SelectStatement:
		select {
		case req := <-db.batchChan:
			lsmEntries := []*lsmDataEntry{}
			for _, entry := range req.entries {
				lsmEntry, err := createDataEntry(entry.ts, entry.key, entry.value)
				if err != nil {
					req.replyChan <- err
					break SelectStatement
				}
				lsmEntries = append(lsmEntries, lsmEntry)
			}
			err := db.mutable.BatchPut(lsmEntries)
			if err != nil {
				req.replyChan <- err
			} else {
				if db.mutable.size > MemTableSize {
					db.flushChan <- db.mutable
					db.mutable, db.immutable = db.immutable, db.mutable
				}
				req.replyChan <- nil
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
			err := db.flush(mt)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}
