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

		if i == 0 {
			keySize := uint8(item[0])
			key := item[1 : 1+keySize]
			indexEntry := createLsmIndex(string(key), currBlock)
			indexBlock = append(indexBlock, indexEntry...)

			currBlock++
		}

		i += copy(block[i:], item)
	}

	dataBlocks = append(dataBlocks, block...)
	header := createHeader(len(dataBlocks), len(indexBlock))
	data := append(header, append(dataBlocks, indexBlock...)...)

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
