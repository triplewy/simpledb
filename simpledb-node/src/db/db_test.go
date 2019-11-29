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
		t.Fatalf("Error inserting into LSM: %v\n", err)
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

func TestDBOverlapPut(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	numItems := 50000
	memoryKV := make(map[string]string)
	entries := []*KV{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		memoryKV[key] = key
		entries = append(entries, &KV{key: key, value: key})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	entries = []*KV{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i + 1)
		memoryKV[key] = value
		entries = append(entries, &KV{key: key, value: value})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error getting from LSM: %v\n", err)
	}
}

func TestDBDelete(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	numItems := 50000
	memoryKV := make(map[string]string)
	entries := []*KV{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		memoryKV[key] = key
		entries = append(entries, &KV{key: key, value: key})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	numCmds := 5000
	entries = []*KV{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		memoryKV[key] = "__delete__"
		entries = append(entries, &KV{key: key, value: nil})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error deleting from LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}
}

func TestDBTinyBenchmark(t *testing.T) {
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
		key := strconv.Itoa(i)
		value := uuid.New().String()
		memoryKV[key] = value
		entries = append(entries, &KV{key: key, value: value})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	numCmds := 10000

	entries = []*KV{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		memoryKV[key] = "__delete__"
		entries = append(entries, &KV{key: key, value: nil})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error deleting from LSM: %v\n", err)
	}

	numCmds = 5000
	entries = []*KV{}
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		value := uuid.New().String()
		memoryKV[key] = value
		entries = append(entries, &KV{key: key, value: value})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}
}

func TestDBRange(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	numItems := 30000
	memoryKV := make(map[string]string)
	entries := []*KV{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		memoryKV[key] = value
		entries = append(entries, &KV{key: key, value: value})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}

	startKey := "0"
	endKey := "9999"

	keys = []string{}
	for key := range memoryKV {
		if startKey <= key && key <= endKey {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	startReadTime := time.Now()
	result, err := db.Range(startKey, endKey, uint64(numItems+1))
	if err != nil {
		t.Fatalf("Error performing range query: %v\n", err)
	}
	duration := time.Since(startReadTime)
	fmt.Printf("Duration reading range: %v\n", duration)

	if len(result) != len(keys) {
		t.Fatalf("Expected range to return %d items, got %d items instead\n", len(keys), len(result))
	}
	numWrong := 0

	for i := 0; i < len(result); i++ {
		key := keys[i]
		got := result[i]
		if key != got.key || memoryKV[key] != got.value.(string) {
			numWrong++
		}
	}

	fmt.Printf("Correct: %f%%\n", float64(numItems-numWrong)/float64(numItems)*float64(100))
}

func TestDBRandom(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	numItems := 10000
	memoryKV := make(map[string]string)
	entries := []*KV{}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(rand.Intn(1000))
		value := strconv.Itoa(i)
		memoryKV[key] = value
		entries = append(entries, &KV{key: key, value: value})
	}

	err = asyncUpdates(db, entries)
	if err != nil {
		t.Fatalf("Error inserting into LSM: %v\n", err)
	}

	keys := []string{}
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, key)
	}

	err = asyncViews(db, keys, memoryKV)
	if err != nil {
		t.Fatalf("Error Reading from LSM: %v\n", err)
	}
}
