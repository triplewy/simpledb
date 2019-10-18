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
		t.Errorf("Error deleting data: %v\n", err)
	}

	lsm, err := NewLSM()
	if err != nil {
		t.Errorf("Error creating LSM: %v\n", err)
	}

	numItems := 1126400

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		err = lsm.Put(key, key)
		if err != nil {
			t.Errorf("Error Puting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	startReadTime := time.Now()
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(1000000000000000000 + rand.Intn(numItems))
		result, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Error reading from LSM: %v\n", err)
		}
		if result != key {
			t.Errorf("Incorrect result from get. Expected: %s, Got: %s\n", key, result)
		}
	}
	duration = time.Since(startReadTime)

	fmt.Printf("Duration reading 10000 random items: %v\n", duration)
	fmt.Printf("Total LSM Read duration: %v, Total Vlog Read duration: %v\n", lsm.totalLsmReadDuration, lsm.totalVlogReadDuration)
}

func TestLSMOverlapPut(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	lsm, err := NewLSM()
	if err != nil {
		t.Errorf("Error creating LSM: %v\n", err)
	}

	numItems := 1000

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		err = lsm.Put(key, key)
		if err != nil {
			t.Errorf("Error Puting into LSM: %v\n", err)
		}
	}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i + 1)
		err = lsm.Put(key, value)
		if err != nil {
			t.Errorf("Error Puting into LSM: %v\n", err)
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
		resultInt, err := strconv.Atoi(result)
		if err != nil {
			t.Errorf("Error converting string to int: %v\n", err)
		}
		if strconv.Itoa(resultInt+1) != key {
			t.Errorf("Incorrect result from get. Expected: %s, Got: %s\n", key, result)
		}
	}
}
