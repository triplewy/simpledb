package db

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestLSMPut(t *testing.T) {
	lsm, err := NewLSM()
	if err != nil {
		t.Errorf("Error creating LSM: %v\n", err)
	}

	for i := 0; i < 300; i++ {
		key := "1000000000000000" + strconv.Itoa(100+i)
		err = lsm.Put(key, key)
		if err != nil {
			t.Errorf("Error Puting into LSM: %v\n", err)
		}
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 300; i++ {
		key := "1000000000000000" + strconv.Itoa(100+i)
		result, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Error reading from LSM: %v\n", err)
		}
		if result != key {
			t.Errorf("Incorrect result from get. Expected: %s, Got: %s\n", key, result)
		}
		fmt.Printf("Result: %s\n", result)
	}
}
