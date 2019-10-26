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

	numWrong := 0
	errors := make(map[string]int)

	startReadTime := time.Now()
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(1000000000000000000 + rand.Intn(numItems))
		result, err := db.Get(key)
		if err != nil {
			numWrong++
			if val, ok := errors[err.Error()]; !ok {
				errors[err.Error()] = 1
			} else {
				errors[err.Error()] = val + 1
			}
		} else if result != key {
			numWrong++
			if val, ok := errors["Incorrect result for get"]; !ok {
				errors["Incorrect result for get"] = 1
			} else {
				errors["Incorrect result for get"] = val + 1
			}
		}
	}
	duration = time.Since(startReadTime)
	fmt.Printf("Duration reading 10000 random items: %v\n", duration)

	fmt.Printf("Correct: %f%%\n", float64(10000-numWrong)/float64(10000)*float64(100))
	if len(errors) > 0 {
		t.Fatalf("Encountered errors during read: %v\n", errors)
	}

	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)
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

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		err = db.Put(key, key)
		if err != nil {
			t.Fatalf("Error Puting into LSM: %v\n", err)
		}
	}
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i + 1)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Error Puting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems*2, duration)

	numWrong := 0
	errors := make(map[string]int)

	startReadTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		result, err := db.Get(key)
		if err != nil {
			numWrong++
			if val, ok := errors[err.Error()]; !ok {
				errors[err.Error()] = 1
			} else {
				errors[err.Error()] = val + 1
			}
		}
		if strconv.Itoa(i+1) != result {
			numWrong++
			if val, ok := errors["Incorrect result for get"]; !ok {
				errors["Incorrect result for get"] = 1
			} else {
				errors["Incorrect result for get"] = val + 1
			}
		}
	}
	duration = time.Since(startReadTime)
	fmt.Printf("Duration reading %d items: %v\n", numItems, duration)

	fmt.Printf("Correct: %f%%\n", float64(10000-numWrong)/float64(10000)*float64(100))
	if len(errors) > 0 {
		t.Fatalf("Encountered errors during read: %v\n", errors)
	}

	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)
}

func TestDBPutUpdate(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating LSM: %v\n", err)
	}

	memoryKV := make(map[string][]string)

	type command struct {
		key   string
		value string
	}

	numCmds := 50000
	commands := []*command{}

	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(1000))
		value := strconv.Itoa(i)

		memoryKV[key] = append(memoryKV[key], value)
		commands = append(commands, &command{key: key, value: value})
	}

	startInsertTime := time.Now()
	for _, cmd := range commands {
		err := db.Put(cmd.key, cmd.value)
		if err != nil {
			t.Fatalf("Error putting data: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numCmds, duration)

	numWrong := 0
	errors := make(map[string]int)

	startReadTime := time.Now()
	for key, value := range memoryKV {
		result, err := db.Get(key)
		if err != nil {
			numWrong++
			if val, ok := errors[err.Error()]; !ok {
				errors[err.Error()] = 1
			} else {
				errors[err.Error()] = val + 1
			}
		}
		if result != value[len(value)-1] {
			t.Errorf("Incorrect result for key: %v. Expected: %v, Got: %v\n", key, value[len(value)-1], result)
			numWrong++
			if val, ok := errors["Incorrect result for get"]; !ok {
				errors["Incorrect result for get"] = 1
			} else {
				errors["Incorrect result for get"] = val + 1
			}
		}
	}
	duration = time.Since(startReadTime)
	fmt.Printf("Duration reading %d items: %v\n", numCmds, duration)

	fmt.Printf("Correct: %f%%\n", float64(10000-numWrong)/float64(10000)*float64(100))
	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)

	if len(errors) > 0 {
		t.Fatalf("Encountered errors during read: %v\n", errors)
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

	memoryKV := make(map[string]string)
	numItems := 50000

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		memoryKV[key] = key
		err := db.Put(key, key)
		if err != nil {
			t.Fatalf("Error inserting into LSM: %v\n", key)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	numCmds := 5000

	startDeleteTime := time.Now()
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		memoryKV[key] = "__delete__"
		err := db.Delete(key)
		if err != nil {
			t.Fatalf("Error deleting from LSM: %v\n", key)
		}
	}
	duration = time.Since(startDeleteTime)
	fmt.Printf("Duration deleting %d items: %v\n", numCmds, duration)

	numWrong := 0
	errors := make(map[string]int)

	startReadTime := time.Now()
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		value := memoryKV[key]

		result, err := db.Get(key)
		if value == "__delete__" {
			if err != nil {
				if err.Error() != "Key not found" {
					numWrong++
					if val, ok := errors[err.Error()]; !ok {
						errors[err.Error()] = 1
					} else {
						errors[err.Error()] = val + 1
					}
				}
			} else {
				numWrong++
				if val, ok := errors["Incorrect result for get"]; !ok {
					errors["Incorrect result for get"] = 1
				} else {
					errors["Incorrect result for get"] = val + 1
				}
			}
		} else {
			if err != nil {
				numWrong++
				if val, ok := errors[err.Error()]; !ok {
					errors[err.Error()] = 1
				} else {
					errors[err.Error()] = val + 1
				}
			} else if value != result {
				numWrong++
				if val, ok := errors["Incorrect result for get"]; !ok {
					errors["Incorrect result for get"] = 1
				} else {
					errors["Incorrect result for get"] = val + 1
				}
			}
		}
	}
	duration = time.Since(startReadTime)

	fmt.Printf("Correct: %f%%\n", float64(numCmds-numWrong)/float64(numCmds)*float64(100))

	if len(errors) > 0 {
		t.Fatalf("Encountered errors during read: %v\n", errors)
	}

	fmt.Printf("Duration reading %d items: %v\n", numCmds, duration)
	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)
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

	memoryKV := make(map[string]string)
	numItems := 100000

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		memoryKV[key] = value
		err := db.Put(key, value)
		if err != nil {
			t.Fatalf("Error inserting into LSM: %v\n", key)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	numCmds := 50000

	startDeleteTime := time.Now()
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		memoryKV[key] = "__delete__"
		err := db.Delete(key)
		if err != nil {
			t.Fatalf("Error deleting from LSM: %v\n", key)
		}
	}
	duration = time.Since(startDeleteTime)
	fmt.Printf("Duration deleting %d items: %v\n", numCmds, duration)

	startUpdateTime := time.Now()
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		value := uuid.New().String()
		memoryKV[key] = value
		err := db.Put(key, value)
		if err != nil {
			t.Fatalf("Error inserting into LSM: %v\n", key)
		}
	}
	duration = time.Since(startUpdateTime)
	fmt.Printf("Duration updating %d items: %v\n", numCmds, duration)

	numWrong := 0
	errors := make(map[string]int)

	startReadTime := time.Now()
	for i := 0; i < numCmds; i++ {
		key := strconv.Itoa(rand.Intn(numItems))
		value := memoryKV[key]

		result, err := db.Get(key)
		if value == "__delete__" {
			if err != nil {
				if err.Error() != "Key not found" {
					numWrong++
					if val, ok := errors[err.Error()]; !ok {
						errors[err.Error()] = 1
					} else {
						errors[err.Error()] = val + 1
					}
				}
			} else {
				numWrong++
				if val, ok := errors["Key should have been deleted"]; !ok {
					errors["Key should have been deleted"] = 1
				} else {
					errors["Key should have been deleted"] = val + 1
				}
			}
		} else {
			if err != nil {
				numWrong++
				if val, ok := errors[err.Error()]; !ok {
					errors[err.Error()] = 1
				} else {
					errors[err.Error()] = val + 1
				}
			} else if value != result {
				numWrong++
				if val, ok := errors["Incorrect result for get"]; !ok {
					errors["Incorrect result for get"] = 1
				} else {
					errors["Incorrect result for get"] = val + 1
				}
			}
		}
	}
	duration = time.Since(startReadTime)
	fmt.Printf("Duration reading %d items: %v\n", numCmds, duration)

	fmt.Printf("Correct: %f%%\n", float64(numCmds-numWrong)/float64(numCmds)*float64(100))
	if len(errors) > 0 {
		t.Fatalf("Encountered errors during read: %v\n", errors)
	}

	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)
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

	memoryKV := make(map[string]string)
	numItems := 50000

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		memoryKV[key] = value
		err := db.Put(key, value)
		if err != nil {
			t.Fatalf("Error inserting into LSM: %v\n", key)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)

	errors := make(map[string]int)
	numWrong := 0

	startReadTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(i)
		value := memoryKV[key]
		result, err := db.Get(key)
		if err != nil {
			numWrong++
			if val, ok := errors[err.Error()]; !ok {
				errors[err.Error()] = 1
			} else {
				errors[err.Error()] = val + 1
			}
		} else if result != value {
			numWrong++
			if val, ok := errors["Incorrect result for get"]; !ok {
				errors["Incorrect result for get"] = 1
			} else {
				errors["Incorrect result for get"] = val + 1
			}
		}
	}
	duration = time.Since(startReadTime)
	fmt.Printf("Duration reading %d items: %v\n", numItems, duration)

	fmt.Printf("Correct: %f%%\n", float64(numItems-numWrong)/float64(numItems)*float64(100))
	if len(errors) > 0 {
		t.Fatalf("Encountered errors during read: %v\n", errors)
	}
	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)

	startKey := "0"
	endKey := "9999"

	expected := []*KVPair{}
	for key, value := range memoryKV {
		if startKey <= key && key <= endKey {
			expected = append(expected, &KVPair{key: key, value: value})
		}
	}

	sort.SliceStable(expected, func(i, j int) bool {
		return expected[i].key < expected[j].key
	})

	startReadTime = time.Now()
	result, err := db.Range(startKey, endKey)
	if err != nil {
		t.Fatalf("Error performing range query: %v\n", err)
	}
	duration = time.Since(startReadTime)
	fmt.Printf("Duration reading range: %v\n", duration)

	if len(result) != len(expected) {
		t.Fatalf("Expected range to return %d items, got %d items instead\n", len(expected), len(result))
	}
	numWrong = 0

	for i := 0; i < len(result); i++ {
		expect := expected[i]
		got := result[i]

		if expect.key != got.key || expect.value != got.value {
			numWrong++
		}
	}

	fmt.Printf("Correct: %f%%\n", float64(numItems-numWrong)/float64(numItems)*float64(100))
	fmt.Printf("Total LSM Read duration: %v\nTotal Vlog Read duration: %v\n", db.totalLsmReadDuration, db.totalVlogReadDuration)
}
