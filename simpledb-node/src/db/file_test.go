package db

import (
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestFileRange(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	entries := []*LSMDataEntry{}

	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(int(math.Pow10(9)) + i)
		entry, err := createDataEntry(uint64(i), key, key)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		entries = append(entries, entry)
	}

	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		t.Fatalf("Error writing data entries: %v\n", err)
	}

	f, err := os.OpenFile("data/L0/test.sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		f.Close()
		t.Fatalf("Error opening file: %v\n", err)
	}

	keyRangeEntry := createKeyRangeEntry(keyRange)
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

	replyChan := make(chan []*LSMDataEntry)
	errChan := make(chan error)
	var wg sync.WaitGroup

	go func() {
		for {
			select {
			case reply := <-replyChan:
				for i := 0; i < len(reply); i++ {
					item := reply[i]
					if string(item.value) != item.key {
						t.Errorf("Value expected: %v, got :%v\n", item.key, string(item.value))
					}
				}
				wg.Done()
			case err := <-errChan:
				t.Fatalf("Error range query on file: %v\n", err)
				wg.Done()
			}
		}
	}()

	wg.Add(1)
	fileRangeQuery("data/L0/test.sst", strconv.Itoa(int(math.Pow10(9))), strconv.Itoa(int(math.Pow10(9))+1000000), replyChan, errChan)

	wg.Wait()
}
