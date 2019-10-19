package db

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestLSMPut(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	lsm, err := NewLSM()
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	numItems := 4096

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		err = lsm.Put(key, key)
		if err != nil {
			t.Fatalf("Error Puting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	startReadTime := time.Now()
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(1000000000000000000 + rand.Intn(numItems))
		result, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("Error reading from LSM: %v\n", err)
		}
		if result != key {
			t.Fatalf("Incorrect result from get. Expected: %s, Got: %s\n", key, result)
		}
	}
	duration = time.Since(startReadTime)

	fmt.Printf("Duration reading 10000 random items: %v\n", duration)
	fmt.Printf("Total LSM Read duration: %v, Total Vlog Read duration: %v\n", lsm.totalLsmReadDuration, lsm.totalVlogReadDuration)
}

func TestLSMOverlapPut(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	lsm, err := NewLSM()
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	numItems := 20480

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		err = lsm.Put(key, key)
		if err != nil {
			t.Fatalf("Error Puting into LSM: %v\n", err)
		}
	}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i + 1)
		err = lsm.Put(key, value)
		if err != nil {
			t.Fatalf("Error Puting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		result, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Error reading from LSM: %v\n", err)
		}
		if strconv.Itoa(i+1) != result {
			t.Errorf("Incorrect result from get. Expected: %s, Got: %s\n", strconv.Itoa(i+1), result)
		}
	}

	fmt.Printf("Total LSM Read duration: %v, Total Vlog Read duration: %v\n", lsm.totalLsmReadDuration, lsm.totalVlogReadDuration)
}
