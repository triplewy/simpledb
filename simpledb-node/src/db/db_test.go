package db

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDBPutOnly(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 10000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		value := key
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < 5000; i++ {
		key := strconv.Itoa(1000000000000000000 + rand.Intn(numItems))
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}
}

func TestDBOverlapPut(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 5000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := key
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	entries = []*Entry{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i + 1)
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}
}

func TestDBDelete(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 20000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := key
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	numCmds := 5000
	keys := []string{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		keys = append(keys, key)
	}

	err = asyncDeletes(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error deleting from LSM: %v\n", err)
	}

	keys = []string{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}
}

func TestDBTinyBenchmark(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 20000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	numCmds := 10000

	keys := []string{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems / 2))
		keys = append(keys, key)
	}
	err = asyncDeletes(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error deleting from LSM: %v\n", err)
	}

	numCmds = 5000
	entries = []*Entry{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems / 10))
		value := uuid.New().String()
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys = []string{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}
}

func TestDBRange(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 20000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}

	startKey := "0"
	endKey := "9999"

	keys = []string{}
	for key := range memorykv {
		if startKey <= key && key <= endKey {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	startReadTime := time.Now()
	err = db.ViewTxn(func(txn *Txn) error {
		entries, err := txn.Scan(startKey, endKey)
		if err != nil {
			return err
		}
		if len(entries) != len(keys) {
			return fmt.Errorf("expected range to return %d items, got %d items instead", len(keys), len(entries))
		}
		numWrong := 0
		for i := 0; i < len(entries); i++ {
			key := keys[i]
			got := entries[i]
			if key != got.Key || memorykv[key] != string(got.Fields["value"].Data) {
				numWrong++
			}
		}
		fmt.Printf("Correct: %f%%\n", float64(numItems-numWrong)/float64(numItems)*float64(100))
		return nil
	})
	if err != nil {
		t.Fatalf("Error performing range query: %v\n", err)
	}
	duration := time.Since(startReadTime)
	fmt.Printf("Duration reading range: %v\n", duration)
}

func TestDBRandom(t *testing.T) {
	db, err := setupDB("data")
	if err != nil {
		t.Fatalf("Error setting up DB: %v\n", err)
	}

	numItems := 10000
	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(rand.Intn(1000))
		value := strconv.Itoa(i)
		entries = append(entries, simpleEntry(uint64(i), key, value))
	}

	err = asyncUpdateTxns(db, entries, memorykv)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViewTxns(db, keys, memorykv)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}
}
