package simpledb

import (
	"fmt"
	"testing"
)

func TestLSMInsert(t *testing.T) {
	lsm, err := NewLSM()
	if err != nil {
		t.Errorf("Error creating LSM: %v\n", err)
	}
	value := "bigboybruv"
	err = lsm.Insert("test", value)
	if err != nil {
		t.Errorf("Error inserting into LSM: %v\n", err)
	}
	result, err := lsm.Get("test")
	if err != nil {
		t.Errorf("Error reading from LSM: %v\n", err)
	}
	if result != value {
		t.Errorf("Incorrect result from get. Expected: %s, Got: %s\n", value, result)
	}
	fmt.Printf("Result: %s\n", result)
}
