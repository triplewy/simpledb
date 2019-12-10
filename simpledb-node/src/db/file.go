package db

import (
	"encoding/binary"
	"os"
)

func writeNewFile(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, filePerm)
	defer f.Close()
	if err != nil {
		return err
	}
	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return newErrWriteUnexpectedBytes(filename)
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

func mmap(filename string) (entries []*Entry, err error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, filePerm)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	dataSize, _, _, _, err := readHeader(f)
	if err != nil {
		return nil, err
	}
	data := make([]byte, dataSize)
	numBytes, err := f.ReadAt(data, headerSize)
	if err != nil {
		return nil, err
	}
	if numBytes != len(data) {
		return nil, newErrReadUnexpectedBytes(filename)
	}
	return decodeEntries(data)
}

// recoverFile reads a file and returns key range, bloom filter, and total size of the file
func recoverFile(filename string) (keyRange *keyRange, bloom *bloom, size int, err error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, filePerm)
	defer f.Close()
	if err != nil {
		return nil, nil, 0, err
	}
	dataSize, indexSize, bloomSize, keyRangeSize, err := readHeader(f)
	if err != nil {
		return nil, nil, 0, err
	}
	bitsAndkeyRange := make([]byte, bloomSize+keyRangeSize)
	numBytes, err := f.ReadAt(bitsAndkeyRange, int64(headerSize+dataSize+indexSize))
	if err != nil {
		return nil, nil, 0, err
	}
	if numBytes != len(bitsAndkeyRange) {
		return nil, nil, 0, newErrWriteUnexpectedBytes(filename)
	}
	bits := bitsAndkeyRange[:bloomSize]
	keyRangeBytes := bitsAndkeyRange[bloomSize:]
	bloom = recoverBloom(bits)
	keyRange = parsekeyRangeEntry(keyRangeBytes)
	return keyRange, bloom, int(dataSize + indexSize + bloomSize + keyRangeSize), nil
}

func fileFind(filename, key string, ts uint64) (entry *Entry, err error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, filePerm)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	dataSize, indexSize, _, _, err := readHeader(f)
	if err != nil {
		return nil, err
	}
	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, headerSize+int64(dataSize))
	if err != nil {
		return nil, err
	}
	if numBytes != int(indexSize) {
		return nil, newErrReadUnexpectedBytes("SST File, Index Block")
	}
	blockIndex, err := findDataBlock(key, index)
	if err != nil {
		return nil, err
	}
	block := make([]byte, BlockSize)
	numBytes, err = f.ReadAt(block, headerSize+int64(BlockSize*int(blockIndex)))
	if err != nil {
		return nil, err
	}
	if numBytes != BlockSize {
		return nil, newErrReadUnexpectedBytes("SST File, Data Block")
	}
	return findKeyInBlock(key, ts, block)
}

func fileRange(filename string, keyRange *keyRange, ts uint64) (entries []*Entry, err error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, filePerm)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	dataSize, indexSize, _, _, err := readHeader(f)
	if err != nil {
		return nil, err
	}
	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, headerSize+int64(dataSize))
	if err != nil {
		return nil, err
	}
	if numBytes != int(indexSize) {
		return nil, newErrReadUnexpectedBytes("SST File, Index Block")
	}
	startBlock, endBlock := rangeDataBlocks(keyRange.startKey, keyRange.endKey, index)
	size := int(endBlock-startBlock+1) * BlockSize
	blocks := make([]byte, size)
	numBytes, err = f.ReadAt(blocks, headerSize+int64(BlockSize*int(startBlock)))
	if err != nil {
		return nil, err
	}
	if numBytes != size {
		return nil, newErrReadUnexpectedBytes("SST File, Data Block")
	}
	return findKeysInBlocks(keyRange, ts, blocks)
}

func findDataBlock(key string, data []byte) (uint32, error) {
	i := 0
	for i < len(data) {
		size := uint8(data[i])
		i++
		if i+int(size) > len(data) {
			break
		}
		indexKey := string(data[i : i+int(size)])
		i += int(size)
		if i+4 > len(data) {
			break
		}
		if key <= indexKey {
			return binary.LittleEndian.Uint32(data[i : i+4]), nil
		}
		i += 4
	}
	return 0, newErrKeyNotFound()
}

func findKeyInBlock(key string, ts uint64, data []byte) (*Entry, error) {
	entries, err := decodeEntries(data)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Key == key && entry.ts < ts {
			return entry, nil
		}
	}
	return nil, newErrKeyNotFound()
}

func rangeDataBlocks(startKey, endKey string, data []byte) (startBlock, endBlock uint32) {
	foundStartBlock := false
	block := uint32(0)
	i := 0
	for i < len(data) {
		size := uint8(data[i])
		i++
		if i+int(size) > len(data) {
			break
		}
		indexKey := string(data[i : i+int(size)])
		i += int(size)
		if i+4 > len(data) {
			break
		}
		block = binary.LittleEndian.Uint32(data[i : i+4])
		i += 4
		if !foundStartBlock && startKey <= indexKey {
			startBlock = block
			foundStartBlock = true
		}
		if endKey <= indexKey {
			return startBlock, block
		}
	}
	return startBlock, block
}

func findKeysInBlocks(keyRange *keyRange, ts uint64, data []byte) (result []*Entry, err error) {
	startKey := keyRange.startKey
	endKey := keyRange.endKey
	entries, err := decodeEntries(data)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, entry := range entries {
		if _, ok := set[entry.Key]; !ok && startKey <= entry.Key && entry.Key <= endKey && entry.ts < ts {
			result = append(result, entry)
		}
	}
	return result, nil
}
