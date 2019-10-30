package db

import (
	"encoding/binary"
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

	values := [][]byte{}

	for i := 0; i < 10000; i++ {
		value := strconv.Itoa(int(math.Pow10(9)) + i)
		valueSize := uint8(len(value))
		offset := uint64(i)
		size := uint32(0)

		offsetBytes := make([]byte, 8)
		sizeBytes := make([]byte, 4)

		binary.LittleEndian.PutUint64(offsetBytes, offset)
		binary.LittleEndian.PutUint32(sizeBytes, size)

		entry := []byte{valueSize}
		entry = append(entry, value...)
		entry = append(entry, offsetBytes...)
		entry = append(entry, sizeBytes...)

		values = append(values, entry)
	}

	f, err := os.OpenFile("data/L0/test.sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		f.Close()
		t.Fatalf("Error opening file: %v\n", err)
	}

	startKeySize := uint8(values[0][0])
	startKey := string(values[0][1 : 1+startKeySize])
	endKeySize := uint8(values[len(values)-1][0])
	endKey := string(values[len(values)-1][1 : 1+endKeySize])
	bloom := NewBloom(len(values))
	indexBlock := []byte{}
	dataBlocks := []byte{}
	block := make([]byte, blockSize)
	currBlock := uint32(0)

	i := 0
	for _, item := range values {
		if i+len(item) > blockSize {
			dataBlocks = append(dataBlocks, block...)
			block = make([]byte, blockSize)
			i = 0
		}

		keySize := uint8(item[0])
		key := string(item[1 : 1+keySize])

		if i == 0 {
			indexEntry := createLsmIndex(key, currBlock)
			indexBlock = append(indexBlock, indexEntry...)
			currBlock++
		}

		bloom.Insert(key)
		i += copy(block[i:], item)
	}

	keyRangeEntry := createKeyRangeEntry(startKey, endKey)
	dataBlocks = append(dataBlocks, block...)
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
					if uint64(i) != item.vlogOffset {
						t.Errorf("Offset expected: %d, got :%d\n", i, item.vlogOffset)
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
