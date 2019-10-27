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

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		err = db.Put(key, key)
		if err != nil {
			t.Fatalf("Error Putting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)
	fmt.Printf("Duration opening vlog: %v\n", db.vlog.openTime)

	db.ForceClose()
}
