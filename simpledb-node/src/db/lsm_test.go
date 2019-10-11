package db

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestLSMPut(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	lsm, err := NewLSM()
	if err != nil {
		t.Errorf("Error creating LSM: %v\n", err)
	}

	startInsertTime := time.Now()
	for i := 0; i < 257; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		err = lsm.Put(key, key)
		if err != nil {
			t.Errorf("Error Puting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Println("Duration inserting 1000 items:", duration)

	for i := 0; i < 257; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		result, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Error reading from LSM: %v\n", err)
		}
		if result != key {
			t.Errorf("Incorrect result from get. Expected: %s, Got: %s\n", key, result)
		}
	}
}
