package db

import (
	"strconv"
	"testing"
)

func TestMergeAbove1(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	ssTable, err := NewSSTable()
	if err != nil {
		t.Errorf("Error creating SSTable: %v\n", err)
	}

	keys1 := []string{}
	keys2 := []string{}
	keys3 := []string{}
	keys4 := []string{}

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		if 1000000000000000000+i%4 == 0 {
			keys1 = append(keys1, key)
		} else if 1000000000000000000+i-1%4 == 0 {
			keys2 = append(keys2, key)
		} else if 1000000000000000000+i-2%4 == 0 {
			keys3 = append(keys3, key)
		} else {
			keys4 = append(keys4, key)
		}
	}

	err = PopulateSSTFile(keys1, "data/L0/test1")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = PopulateSSTFile(keys2, "data/L0/test2")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = PopulateSSTFile(keys3, "data/L0/test3")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = PopulateSSTFile(keys4, "data/L0/test4")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}

	result, err := ssTable.level1.mergeAbove([]string{"data/L0/test2", "data/L0/test1", "data/L0/test3", "data/L0/test4"})
	if err != nil {
		t.Errorf("Error merge sorting files: %v\n", err)
	}

	i := 0
	for _, item := range result {
		keySize := uint(item[0])
		key := item[1 : 1+keySize]
		if string(key) != strconv.Itoa(1000000000000000000+i) {
			t.Errorf("Did not sort files properly. Expected: %s, Got: %s\n", strconv.Itoa(1000000000000000000+i), string(key))
		}
		i++
	}
}

func TestMergeAbove2(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	ssTable, err := NewSSTable()
	if err != nil {
		t.Errorf("Error creating SSTable: %v\n", err)
	}

	keys1 := []string{}
	keys2 := []string{}
	keys3 := []string{}

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		if 1000000000000000000+i%4 == 0 {
			keys1 = append(keys1, key)
		} else if 1000000000000000000+i-1%4 == 0 {
			keys2 = append(keys2, key)
		} else {
			keys3 = append(keys3, key)
		}
	}

	err = PopulateSSTFile(keys1, "data/L0/test1")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = PopulateSSTFile(keys2, "data/L0/test2")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = PopulateSSTFile(keys3, "data/L0/test3")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}

	result, err := ssTable.level1.mergeAbove([]string{"data/L0/test2", "data/L0/test1", "data/L0/test3"})
	if err != nil {
		t.Errorf("Error merge sorting files: %v\n", err)
	}

	i := 0
	for _, item := range result {
		keySize := uint(item[0])
		key := item[1 : 1+keySize]
		if string(key) != strconv.Itoa(1000000000000000000+i) {
			t.Errorf("Did not sort files properly. Expected: %s, Got: %s\n", strconv.Itoa(1000000000000000000+i), string(key))
		}
		i++
	}
}
