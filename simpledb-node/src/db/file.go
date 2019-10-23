package db

import (
	"encoding/binary"
	"os"
)

func fileFind(filename, key string, replyChan chan *LSMDataEntry, errChan chan error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		errChan <- err
		return
	}

	dataSize, indexSize, err := readHeader(f)
	if err != nil {
		errChan <- err
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, 16+int64(dataSize))
	if err != nil {
		errChan <- err
		return
	}

	if numBytes != int(indexSize) {
		errChan <- newErrReadUnexpectedBytes("SST File, Index Block")
		return
	}

	blockIndex := findDataBlock(key, index)

	block := make([]byte, blockSize)
	numBytes, err = f.ReadAt(block, 16+int64(blockSize*int(blockIndex)))
	if err != nil {
		errChan <- err
		return
	}
	if numBytes != blockSize {
		errChan <- newErrReadUnexpectedBytes("SST File, Data Block")
		return
	}

	result, err := findKeyInBlock(key, block)
	if err != nil {
		errChan <- err
		return
	}
	replyChan <- result
}

func fileRangeQuery(filename, startKey, endKey string, replyChan chan []*LSMDataEntry, errChan chan error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		errChan <- err
		return
	}

	dataSize, indexSize, err := readHeader(f)
	if err != nil {
		errChan <- err
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, 16+int64(dataSize))
	if err != nil {
		errChan <- err
		return
	}
	if numBytes != int(indexSize) {
		errChan <- newErrReadUnexpectedBytes("SST File, Index Block")
		return
	}

	startBlock, endBlock := rangeDataBlocks(startKey, endKey, index)

	size := int(endBlock+1)*blockSize - int(startBlock)*blockSize
	blocks := make([]byte, size)

	numBytes, err = f.ReadAt(blocks, 16+int64(blockSize*int(startBlock)))
	if err != nil {
		errChan <- err
		return
	}
	if numBytes != size {
		errChan <- newErrReadUnexpectedBytes("SST File, Data Block")
		return
	}

	lsmEntries := findKeysInBlocks(startKey, endKey, blocks)
	replyChan <- lsmEntries
}

func findDataBlock(key string, index []byte) uint32 {
	i := 0
	block := uint32(0)
	for i < len(index) {
		size := uint8(index[i])
		i++
		indexKey := string(index[i : i+int(size)])
		i += int(size)

		if key < indexKey {
			return binary.LittleEndian.Uint32(index[i:i+4]) - 1
		} else if key == indexKey {
			return binary.LittleEndian.Uint32(index[i : i+4])
		}
		i += 4
		block++
	}

	return block - 1
}

func findKeyInBlock(key string, block []byte) (*LSMDataEntry, error) {
	i := 0

	for i < len(block) && block[i] != byte(0) {
		keySize := uint8(block[i])
		i++
		foundKey := string(block[i : i+int(keySize)])
		i += int(keySize)
		if key == foundKey {
			vlogOffset := binary.LittleEndian.Uint64(block[i : i+8])
			i += 8
			vlogSize := binary.LittleEndian.Uint32(block[i : i+4])
			i += 4
			return &LSMDataEntry{
				keySize:    keySize,
				key:        key,
				vlogOffset: vlogOffset,
				vlogSize:   vlogSize,
			}, nil
		}
		i += 12
	}

	return nil, newErrKeyNotFound()
}

func rangeDataBlocks(startKey, endKey string, index []byte) (startBlock, endBlock uint32) {
	foundStart := false

	i := 0
	block := uint32(0)

	for i < len(index) {
		size := uint8(index[i])
		i++
		indexKey := string(index[i : i+int(size)])
		i += int(size)

		if indexKey == startKey {
			startBlock = binary.LittleEndian.Uint32(index[i : i+4])
			foundStart = true
		} else if indexKey == endKey {
			endBlock = binary.LittleEndian.Uint32(index[i : i+4])
			return startBlock, endBlock
		} else if startKey < indexKey && indexKey < endKey {
			if !foundStart {
				start := binary.LittleEndian.Uint32(index[i : i+4])
				if start == 0 {
					startBlock = uint32(0)
				} else {
					startBlock = start - 1
				}
				foundStart = true
			}
		} else if indexKey > endKey {
			endBlock = binary.LittleEndian.Uint32(index[i:i+4]) - 1
			return startBlock, endBlock
		}
		i += 4
		block++
	}
	return startBlock, block - 1
}

func findKeysInBlocks(startKey, endKey string, data []byte) []*LSMDataEntry {
	result := []*LSMDataEntry{}

	for i := 0; i < len(data); i += blockSize {
		block := data[i : i+blockSize]
		j := 0
		for j < len(block) && block[j] != byte(0) {
			keySize := uint8(block[j])
			j++
			key := string(block[j : j+int(keySize)])
			j += int(keySize)

			if key < startKey {
				j += 12
			} else if startKey <= key && key <= endKey {
				vlogOffset := binary.LittleEndian.Uint64(block[j : j+8])
				j += 8
				vlogSize := binary.LittleEndian.Uint32(block[j : j+4])
				j += 4
				result = append(result, &LSMDataEntry{
					keySize:    keySize,
					key:        key,
					vlogOffset: vlogOffset,
					vlogSize:   vlogSize,
				})
			} else {
				return result
			}
		}
	}

	return result
}
