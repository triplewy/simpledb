package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unicode"

	"github.com/hashicorp/raft"
	db "github.com/triplewy/simpledb-embedded"
	simpledb "github.com/triplewy/simpledb-embedded"
)

type store struct {
	dir string
	db  *db.DB
}

func newStore(dir string) (*store, error) {
	store := &store{
		dir: dir,
		db:  nil,
	}
	err := store.initialize()
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (store *store) initialize() error {
	err := os.MkdirAll(filepath.Join(store.dir, "data"), dirPerm)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Join(store.dir, "snapshots"), dirPerm)
	if err != nil {
		return err
	}
	db, err := db.NewDB(filepath.Join(store.dir, "data"))
	if err != nil {
		return err
	}
	store.db = db
	return nil
}

// FirstIndex returns the first index written. 0 for no entries.
func (store *store) FirstIndex() (uint64, error) {
	var result uint64
	err := store.db.ViewTxn(func(txn *simpledb.Txn) error {
		startKey := string(append([]byte("log"), uint64ToBytes(0)...))
		endKey := "log" + string(unicode.MaxASCII)
		entries, err := txn.Scan(startKey, endKey)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return errors.New("No logs in db")
		}
		entry := entries[0]
		if value, ok := entry.Attributes["value"]; ok {
			var log *raft.Log
			err := decodeMsgPack(value.Data, log)
			if err != nil {
				return err
			}
			result = log.Index
			return nil
		}
		return errors.New("Log entry has no 'value' field")
	})
	return result, err
}

// LastIndex returns the last index written. 0 for no entries.
func (store *store) LastIndex() (uint64, error) {
	var result uint64
	err := store.db.ViewTxn(func(txn *simpledb.Txn) error {
		startKey := string(append([]byte("log"), uint64ToBytes(0)...))
		endKey := "log" + string(unicode.MaxASCII)
		entries, err := txn.Scan(startKey, endKey)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return errors.New("No logs in db")
		}
		entry := entries[len(entries)-1]
		if value, ok := entry.Attributes["value"]; ok {
			var log *raft.Log
			err := decodeMsgPack(value.Data, log)
			if err != nil {
				return err
			}
			result = log.Index
			return nil
		}
		return errors.New("Log entry has no 'value' field")
	})
	return result, err
}

// GetLog gets a log entry at a given index.
func (store *store) GetLog(index uint64, log *raft.Log) error {
	return store.db.ViewTxn(func(txn *simpledb.Txn) error {
		key := string(append([]byte("log"), uint64ToBytes(log.Index)...))
		entry, err := txn.Read(key)
		if err != nil {
			return err
		}
		if value, ok := entry.Attributes["value"]; ok {
			return decodeMsgPack(value.Data, log)
		}
		return errors.New("Log entry has no 'value' field")
	})
}

// StoreLog stores a log entry.
func (store *store) StoreLog(log *raft.Log) error {
	return store.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (store *store) StoreLogs(logs []*raft.Log) error {
	return store.db.UpdateTxn(func(txn *simpledb.Txn) error {
		for _, log := range logs {
			key := string(append([]byte("log"), uint64ToBytes(log.Index)...))
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}
			txn.Write(key, map[string]*simpledb.Value{"value": &simpledb.Value{
				DataType: simpledb.Bytes,
				Data:     val.Bytes(),
			}})
		}
		return nil
	})
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (store *store) DeleteRange(min, max uint64) error {
	return store.db.UpdateTxn(func(txn *simpledb.Txn) error {
		startKey := string(append([]byte("log"), uint64ToBytes(min)...))
		endKey := string(append([]byte("log"), uint64ToBytes(max)...))
		entries, err := txn.Scan(startKey, endKey)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			txn.Delete(entry.Key)
		}
		return nil
	})
}

// Set is used to set a key/value set outside of the raft log
func (store *store) Set(key []byte, val []byte) error {
	return store.db.UpdateTxn(func(txn *simpledb.Txn) error {
		values := map[string]*simpledb.Value{"value": &simpledb.Value{
			DataType: simpledb.Bytes,
			Data:     val,
		}}
		txn.Write(string(key), values)
		return nil
	})
}

// Get is used to retrieve a value from the k/v store by key
func (store *store) Get(key []byte) ([]byte, error) {
	var result []byte
	err := store.db.ViewTxn(func(txn *simpledb.Txn) error {
		entry, err := txn.Read(string(key))
		if err != nil {
			return err
		}
		if value, ok := entry.Attributes["value"]; ok {
			result = append([]byte{}, value.Data...)
		} else {
			return fmt.Errorf("No 'value' field for key: %v", string(key))
		}
		return nil
	})
	return result, err
}

// SetUint64 is like Set, but handles uint64 values
func (store *store) SetUint64(key []byte, val uint64) error {
	return store.db.UpdateTxn(func(txn *simpledb.Txn) error {
		valBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valBytes, val)
		values := map[string]*simpledb.Value{"value": &simpledb.Value{
			DataType: simpledb.Int,
			Data:     valBytes,
		}}
		txn.Write(string(key), values)
		return nil
	})
}

// GetUint64 is like Get, but handles uint64 values
func (store *store) GetUint64(key []byte) (uint64, error) {
	var result uint64
	err := store.db.ViewTxn(func(txn *simpledb.Txn) error {
		entry, err := txn.Read(string(key))
		if err != nil {
			return err
		}
		if value, ok := entry.Attributes["value"]; ok {
			if value.DataType != simpledb.Int {
				return fmt.Errorf("Wrong DataType for key. Expected: int, Got: %d", value.DataType)
			}
			result = binary.LittleEndian.Uint64(value.Data)
		} else {
			return fmt.Errorf("No 'value' field for key: %v", string(key))
		}
		return nil
	})
	return result, err
}
