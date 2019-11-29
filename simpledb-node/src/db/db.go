package db

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

// DB is struct for database
type DB struct {
	oracle *Oracle

	mutable   *MemTable
	immutable *MemTable

	batchChan chan *batchRequest
	flushChan chan *MemTable

	lsm *LSM

	close chan struct{}
}

type batchRequest struct {
	entries   []*KV
	replyChan chan error
}

// KV is struct for Key-Value pair
type KV struct {
	ts    uint64
	key   string
	value interface{}
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
	memtable1, maxCommitTs1, err := NewMemTable(directory, "1")
	if err != nil {
		return nil, err
	}
	memtable2, maxCommitTs2, err := NewMemTable(directory, "2")
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
		flushChan: make(chan *MemTable),

		lsm: lsm,

		close: make(chan struct{}, 1),
	}

	oracle := NewOracle(maxCommitTs, db)
	db.oracle = oracle

	go db.run()
	go db.runFlush()

	return db, nil
}

// NewTxn returns a new Txn to perform ops on
func (db *DB) NewTxn() *Txn {
	startTs := db.oracle.requestStart()
	return &Txn{
		db:         db,
		startTs:    startTs,
		writeCache: make(map[string]*KV),
		readSet:    make(map[string]uint64),
	}
}

// View implements a read only transaction to the DB. Ensures read only since it does not commit at end
func (db *DB) View(fn func(txn *Txn) error) error {
	txn := db.NewTxn()
	return fn(txn)
}

// Update implements a read and write only transaction to the DB
func (db *DB) Update(fn func(txn *Txn) error) error {
	txn := db.NewTxn()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

// BatchPut inserts multiple key-value pairs into DB
func (db *DB) BatchPut(entries []*KV) error {
	replyChan := make(chan error, 1)
	req := &batchRequest{
		entries:   entries,
		replyChan: replyChan,
	}

	db.batchChan <- req
	return <-replyChan
}

// Get retrieves value for a given key or returns key not found
func (db *DB) Get(key string, ts uint64) (*KV, error) {
	if len(key) > KeySize {
		return nil, NewErrExceedMaxKeySize(key)
	}
	entry := db.mutable.table.Find(key, ts)
	if entry != nil {
		return parseDataEntry(entry)
	}
	entry = db.immutable.table.Find(key, ts)
	if entry != nil {
		return parseDataEntry(entry)
	}
	entry, err := db.lsm.Find(key, ts)
	if err != nil {
		return nil, err
	}
	return parseDataEntry(entry)
}

// Range finds all key, value pairs within the given range of keys
func (db *DB) Range(startKey, endKey string, ts uint64) ([]*KV, error) {
	if len(startKey) > KeySize {
		return nil, NewErrExceedMaxKeySize(startKey)
	}
	if len(endKey) > KeySize {
		return nil, NewErrExceedMaxKeySize(endKey)
	}
	if startKey > endKey {
		return nil, errors.New("Start Key is greater than End Key")
	}
	keyRange := &KeyRange{startKey: startKey, endKey: endKey}

	all := []*LSMDataEntry{}
	all = append(all, db.mutable.table.Range(keyRange, ts)...)
	all = append(all, db.immutable.table.Range(keyRange, ts)...)

	entries, err := db.lsm.Range(keyRange, ts)
	if err != nil {
		return nil, err
	}
	all = append(all, entries...)

	// Convert slice of entries into a set of entries
	resultMap := make(map[string]*LSMDataEntry)
	for _, entry := range all {
		if value, ok := resultMap[entry.key]; !ok {
			resultMap[entry.key] = entry
		} else {
			if value.ts < entry.ts {
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
	SelectStatement:
		select {
		case req := <-db.batchChan:
			lsmEntries := []*LSMDataEntry{}
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
			err := db.Flush(mt)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}
