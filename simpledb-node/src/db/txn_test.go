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
	err := deleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
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
	err := deleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	db.UpdateTxn(func(txn *Txn) error {
		txn.Write("test", map[string]*Value{"value": &Value{dataType: String, data: []byte("test")}})
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
			txn.Write("test", map[string]*Value{"value": &Value{dataType: String, data: []byte(string(entry.fields["value"].data) + " 1")}})
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
			fmt.Println("Read:", entry)
			txn.Write("test", map[string]*Value{"value": &Value{dataType: String, data: []byte(string(entry.fields["value"].data) + " 2")}})
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
	if !(string(result.fields["value"].data) == "test 1" || string(result.fields["value"].data) == "test 2") {
		t.Fatalf("Wrong result from read Txn. Got: %v\n", result)
	}
	fmt.Println(result)
}

func TestTxnAbortWRW(t *testing.T) {
	err := deleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	db.UpdateTxn(func(txn *Txn) error {
		txn.Write("test", map[string]*Value{"value": &Value{dataType: String, data: []byte("test")}})
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
			txn.Write("test", map[string]*Value{"value": &Value{dataType: String, data: []byte("foo")}})
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
			txn.Write("test", map[string]*Value{"value": &Value{dataType: String, data: []byte(string(entry.fields["value"].data) + " test")}})
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
	if !(string(result.fields["value"].data) == "test test" || string(result.fields["value"].data) == "foo") {
		t.Fatalf("Wrong result from read Txn. Got: %v\n", result)
	}
	fmt.Println(result)
}
