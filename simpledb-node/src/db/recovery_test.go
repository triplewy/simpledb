package db

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestRecoverLevels(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 100000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		value := key
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into lsm: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < 50000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error getting from lsm: %v\n", err)
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

	err = asyncViewTxns(newDb, keys, memorykv)
	if err != nil {
		t.Fatalf("Error getting from lsm: %v\n", err)
	}
}

func TestRecoverUnexpected(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 50000
	memorykv := make(map[string]string)
	closeChan := make(chan struct{}, 1)
	success := true
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		time.Sleep(5 * time.Second)
		closeChan <- struct{}{}
	}()

	go func() {
		defer wg.Done()
		<-closeChan
		db.Close()
		newDb, err := NewDB("data")
		if err != nil {
			fmt.Printf("Error creating DB: %v\n", err)
			success = false
			return
		}
		keys := []string{}
		for i := 0; i < 50000; i++ {
			key := strconv.Itoa(1000000000000000000 + i)
			keys = append(keys, key)
		}
		err = asyncViewTxns(newDb, keys, memorykv)
		if err != nil {
			fmt.Printf("Error reading from lsm: %v\n", err)
			success = false
			return
		}
	}()

	go func() {
		for i := 0; i < numItems; i++ {
			key := strconv.Itoa(1000000000000000000 + i)
			err := db.UpdateTxn(func(txn *Txn) error {
				txn.Write(key, map[string]*Value{"value": &Value{DataType: String, Data: []byte(key)}})
				return nil
			})
			if err != nil {
				t.Fatalf("Error inserting into lsm: %v\n", err)
			} else {
				memorykv[key] = key
			}
		}
	}()

	wg.Wait()
	if !success {
		t.Fatalf("Error recovering lsm")
	}
}
