package db

import (
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestFileGet(t *testing.T) {
	err := deleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	entries := []*Entry{}

	for i := 1000; i < 10000; i++ {
		key := strconv.Itoa(i)
		entry, err := createEntry(uint64(i), key, key)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		entries = append(entries, entry)
	}

	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		t.Fatalf("Error writing data entries: %v\n", err)
	}

	keyRangeEntry := createkeyRangeEntry(keyRange)
	header := createHeader(len(dataBlocks), len(indexBlock), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(dataBlocks, indexBlock...), bloom.bits...), keyRangeEntry...)...)

	err = writeNewFile("data/L0/test.sst", data)
	if err != nil {
		t.Fatalf("Error writing to file: %v\n", err)
	}

	var wg sync.WaitGroup
	replyChan := make(chan *Entry)
	errChan := make(chan error)
	errs := make(map[string]int)

	go func() {
		for {
			select {
			case entry := <-replyChan:
				kv, err := parseDataEntry(entry)
				if err != nil {
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				} else if kv.key != kv.value.(string) {
					if _, ok := errs["Incorrect Key Value"]; !ok {
						errs["Incorrect Key Value"] = 1
					} else {
						errs["Incorrect Key Value"]++
					}
				}
				wg.Done()
			case err := <-errChan:
				if _, ok := errs[err.Error()]; !ok {
					errs[err.Error()] = 1
				} else {
					errs[err.Error()]++
				}
				wg.Done()
			}
		}
	}()

	for i := 1000; i < 10000; i++ {
		wg.Add(1)
		key := strconv.Itoa(i)
		go func(key string) {
			entry, err := fileFind("data/L0/test.sst", key, uint64(10001))
			if err != nil {
				errChan <- err
			} else {
				replyChan <- entry
			}
		}(key)

	}

	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("Encountered errors during file get: %v\n", errs)
	}
}
func TestFileRange(t *testing.T) {
	err := deleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	entries := []*Entry{}

	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(int(math.Pow10(9)) + i)
		entry, err := createEntry(uint64(i), key, key)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		entries = append(entries, entry)
	}

	dataBlocks, indexBlock, bloom, kr, err := writeDataEntries(entries)
	if err != nil {
		t.Fatalf("Error writing data entries: %v\n", err)
	}

	f, err := os.OpenFile("data/L0/test.sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		f.Close()
		t.Fatalf("Error opening file: %v\n", err)
	}

	keyRangeEntry := createkeyRangeEntry(kr)
	header := createHeader(len(dataBlocks), len(indexBlock), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(dataBlocks, indexBlock...), bloom.bits...), keyRangeEntry...)...)

	numBytes, err := f.Write(data)
	if err != nil {
		f.Close()
		t.Fatalf("Error writing to file: %v\n", err)
	}
	if numBytes != len(data) {
		if err != nil {
			f.Close()
			t.Fatalf("Num bytes written to file does not match data\n")
		}
	}
	f.Close()

	kr = &keyRange{startKey: strconv.Itoa(int(math.Pow10(9))), endKey: strconv.Itoa(int(math.Pow10(9)) + 1000000)}
	entries, err = fileRange("data/L0/test.sst", kr, uint64(10001))
	if err != nil {
		t.Fatalf("Error range query on file: %v\n", err)
	}

	for _, item := range entries {
		if string(item.value) != item.key {
			t.Errorf("Value expected: %v, got :%v\n", item.key, string(item.value))
		}
	}
}
