package db

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestTxnRead(t *testing.T) {
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
		memorykv[key] = key

	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error updating DB: %v\n", err)
	}

	fmt.Println(db.oracle.commitedTxns.maxTs, db.oracle.commitedTxns.size)

	keys := []string{}
	for i := 0; i < 50000; i++ {
		key := strconv.Itoa(1000000000000000000 + rand.Intn(numItems))
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}
}

func TestTxnAbortRWRW(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	db.UpdateTxn(func(txn *Txn) error {
		txn.Write("test", map[string]*Value{"value": &Value{DataType: String, Data: []byte("test")}})
		return nil
	})

	errChan := make(chan error)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					fmt.Println(err)
				}
				wg.Done()
			}
		}
	}()

	go func() {
		err := db.UpdateTxn(func(txn *Txn) error {
			entry, err := txn.Read("test")
			if err != nil {
				return err
			}
			txn.Write("test", map[string]*Value{"value": &Value{DataType: String, Data: []byte(string(entry.Fields["value"].Data) + " 1")}})
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		errChan <- err
	}()

	go func() {
		time.Sleep(50 * time.Millisecond)
		err := db.UpdateTxn(func(txn *Txn) error {
			time.Sleep(200 * time.Millisecond)
			entry, err := txn.Read("test")
			if err != nil {
				return err
			}
			txn.Write("test", map[string]*Value{"value": &Value{DataType: String, Data: []byte(string(entry.Fields["value"].Data) + " 2")}})
			return nil
		})
		errChan <- err
	}()

	wg.Wait()

	var result *Entry
	err = db.ViewTxn(func(txn *Txn) error {
		entry, err := txn.Read("test")
		if err != nil {
			return err
		}
		result = entry
		return nil
	})
	if err != nil {
		t.Fatalf("Error reading from DB: %v\n", err)
	}
	if !(string(result.Fields["value"].Data) == "test 1" || string(result.Fields["value"].Data) == "test 2") {
		t.Fatalf("Wrong result from read Txn. Got: %v\n", string(result.Fields["value"].Data))
	}
}

func TestTxnAbortWRW(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	db.UpdateTxn(func(txn *Txn) error {
		txn.Write("test", map[string]*Value{"value": &Value{DataType: String, Data: []byte("test")}})
		return nil
	})

	errChan := make(chan error)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					fmt.Println(err)
				}
				wg.Done()
			}
		}
	}()

	go func() {
		err := db.UpdateTxn(func(txn *Txn) error {
			txn.Write("test", map[string]*Value{"value": &Value{DataType: String, Data: []byte("foo")}})
			fmt.Println("finished W")
			return nil
		})
		errChan <- err
	}()

	go func() {
		err := db.UpdateTxn(func(txn *Txn) error {
			entry, err := txn.Read("test")
			if err != nil {
				return err
			}
			time.Sleep(800 * time.Millisecond)
			txn.Write("test", map[string]*Value{"value": &Value{DataType: String, Data: []byte(string(entry.Fields["value"].Data) + " test")}})
			fmt.Println("finished RW")
			return nil
		})
		errChan <- err
	}()

	wg.Wait()

	var result *Entry
	err = db.ViewTxn(func(txn *Txn) error {
		entry, err := txn.Read("test")
		if err != nil {
			return err
		}
		result = entry
		return nil
	})
	if err != nil {
		t.Fatalf("Error reading from DB: %v\n", err)
	}
	if !(string(result.Fields["value"].Data) == "test test" || string(result.Fields["value"].Data) == "foo") {
		t.Fatalf("Wrong result from read Txn. Got: %v\n", string(result.Fields["value"].Data))
	}
}
