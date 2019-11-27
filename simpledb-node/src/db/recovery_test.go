package db

import (
	"strconv"
	"testing"
	"time"
)

func TestRecoverLevels(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating DB: %v\n", err)
	}

	numItems := 100000
	memoryKV := make(map[string]string)
	entries := []*KV{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		memoryKV[key] = key
		entries = append(entries, &KV{key: key, value: key})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < 50000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}

	db.Close()

	newDb, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating DB: %v\n", err)
	}

	keys = []string{}
	for i := 0; i < 50000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		keys = append(keys, key)
	}

	err = asyncViews(newDb, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}
}

func TestRecoverUnexpected(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating DB: %v\n", err)
	}

	numItems := 50000
	memoryKV := make(map[string]string)
	closeChan := make(chan struct{}, 1)

	go func() {
		time.Sleep(5 * time.Second)
		closeChan <- struct{}{}
	}()

	go func() {
		<-closeChan
		db.Close()
		newDb, err := NewDB("data")
		if err != nil {
			t.Fatalf("Error creating DB: %v\n", err)
		}
		keys := []string{}
		for i := 0; i < 50000; i++ {
			key := strconv.Itoa(1000000000000000000 + i)
			keys = append(keys, key)
		}

		err = asyncViews(newDb, keys, memoryKV)
		if err != nil {
			t.Fatalf("Error reading from LSM: %v", err)
		}
		return
	}()

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		err := db.Update(func(txn *Txn) error {
			txn.Write(key, key)
			return nil
		})
		if err != nil {
			t.Fatalf("Error inserting into LSM: %v\n", err)
		} else {
			memoryKV[key] = key
		}
	}
}
