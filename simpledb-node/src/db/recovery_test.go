package db

import (
	"fmt"
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

	numItems := 50000
	memoryKV := make(map[string]string)

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		memoryKV[key] = key
		err = db.Put(key, key)
		if err != nil {
			t.Fatalf("Error Putting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		result, err := db.Get(key)
		if err != nil {
			t.Fatalf("Error Putting into LSM: %v\n", err)
		}
		if result != memoryKV[key] {
			t.Fatalf("Incorrect result from db Get")
		}
	}

	db.Close()

	newDb, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating DB: %v\n", err)
	}
	for fileID, keyRange := range newDb.lsm.levels[3].manifest {
		fmt.Println(fileID, keyRange.startKey, keyRange.endKey)
	}

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		result, err := newDb.Get(key)
		if err != nil {
			fmt.Println(key)
			t.Fatalf("Error Getting from LSM: %v\n", err)
		}
		if result != memoryKV[key] {
			t.Fatalf("Wrong result\n")
		}
	}
}
