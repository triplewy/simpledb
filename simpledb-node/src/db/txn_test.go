package db

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestTxnRead(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
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
		t.Fatalf("Error updating DB: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < 50000; i++ {
		key := strconv.Itoa(1000000000000000000 + rand.Intn(numItems))
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}
}
