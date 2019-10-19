package db

import (
	"errors"
	"strconv"
	"time"
)

// DB is struct for database
type DB struct {
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

	return &DB{
		lsm:  lsm,
		vlog: vlog,

		totalLsmReadDuration:  0 * time.Second,
		totalVlogReadDuration: 0 * time.Second,
	}, nil
}

func (db *DB) Put(key, value string) error {
	if len(key) > keySize {
		return errors.New("Key size has exceeded " + strconv.Itoa(keySize) + " bytes")
	}
	if len(value) > valueSize {
		return errors.New("Value size has exceeded " + strconv.Itoa(valueSize) + " bytes")
	}

	return db.lsm.Put(key, value)
}
