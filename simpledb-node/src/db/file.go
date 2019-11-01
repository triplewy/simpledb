package db

import (
	"encoding/binary"
	"fmt"
	"os"
)

func writeNewFile(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		return err
	}
	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return ErrWriteUnexpectedBytes(filename)
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

// RecoverFile reads a file and returns all index entries, a bloom filter, and total size of the file
func RecoverFile(filename string) (entries []*LSMIndexEntry, bloom *Bloom, size int, err error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, nil, 0, err
	}

	dataSize, indexSize, bloomSize, _, err := readHeader(f)
	if err != nil {
		return nil, nil, 0, err
	}

	fmt.Println("RecoverFile:", filename, indexSize, bloomSize)
	indexAndBloom := make([]byte, indexSize+bloomSize)

	numBytes, err := f.ReadAt(indexAndBloom, int64(headerSize+dataSize))
	if err != nil {
		return nil, nil, 0, err
	}
	if numBytes != len(indexAndBloom) {
		return nil, nil, 0, ErrWriteUnexpectedBytes(filename)
	}

	index := indexAndBloom[:indexSize]
	i := 0
	for i < len(index) {
		keySize := uint8(index[i])
		i++
		key := string(index[i : i+int(keySize)])
		i += int(keySize)
		block := binary.LittleEndian.Uint32(index[i : i+4])
		i += 4
		entries = append(entries, &LSMIndexEntry{
			keySize: keySize,
			key:     key,
			block:   block,
		})
	}

	bits := indexAndBloom[indexSize:]
	bloom = RecoverBloom(bits)

	return entries, bloom, int(dataSize + indexSize + bloomSize), nil
}

func fileFind(filename, key string, replyChan chan *LSMDataEntry, errChan chan error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		errChan <- err
		return
	}

	dataSize, indexSize, _, _, err := readHeader(f)
	if err != nil {
		errChan <- err
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, headerSize+int64(dataSize))
	if err != nil {
		errChan <- err
		return
	}

	if numBytes != int(indexSize) {
		errChan <- ErrReadUnexpectedBytes("SST File, Index Block")
		return
	}

	blockIndex, err := findDataBlock(key, index)
	if err != nil {
		errChan <- err
		return
	}

	block := make([]byte, BlockSize)
	numBytes, err = f.ReadAt(block, headerSize+int64(BlockSize*int(blockIndex)))
	if err != nil {
		errChan <- err
		return
	}
	if numBytes != BlockSize {
		errChan <- ErrReadUnexpectedBytes("SST File, Data Block")
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

	dataSize, indexSize, _, _, err := readHeader(f)
	if err != nil {
		errChan <- err
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, headerSize+int64(dataSize))
	if err != nil {
		errChan <- err
		return
	}
	if numBytes != int(indexSize) {
		errChan <- ErrReadUnexpectedBytes("SST File, Index Block")
		return
	}

	startBlock, endBlock := rangeDataBlocks(startKey, endKey, index)

	size := int(endBlock+1)*BlockSize - int(startBlock)*BlockSize
	blocks := make([]byte, size)

	numBytes, err = f.ReadAt(blocks, headerSize+int64(BlockSize*int(startBlock)))
	if err != nil {
		errChan <- err
		return
	}
	if numBytes != size {
		errChan <- ErrReadUnexpectedBytes("SST File, Data Block")
		return
	}

	lsmEntries := findKeysInBlocks(startKey, endKey, blocks)
	replyChan <- lsmEntries
}

func findDataBlock(key string, index []byte) (uint32, error) {
	i := 0
	for i < len(index) {
		size := uint8(index[i])
		i++
		indexKey := string(index[i : i+int(size)])
		i += int(size)
		if key <= indexKey {
			return binary.LittleEndian.Uint32(index[i : i+4]), nil
		}
		i += 4
	}
	return 0, ErrKeyNotFound()
}

func findKeyInBlock(key string, block []byte) (*LSMDataEntry, error) {
	i := 0
	for i < len(block) {
		seqID := binary.LittleEndian.Uint64(block[i : i+8])
		i += 8
		keySize := uint8(block[i])
		i++
		foundKey := string(block[i : i+int(keySize)])
		i += int(keySize)
		valueType := uint8(block[i])
		i++
		valueSize := binary.LittleEndian.Uint16(block[i : i+2])
		i += 2
		value := block[i : i+int(valueSize)]
		i += int(valueSize)

		if key == foundKey {
			return &LSMDataEntry{
				seqID:     seqID,
				keySize:   keySize,
				key:       key,
				valueType: valueType,
				valueSize: valueSize,
				value:     value,
			}, nil
		}
	}
	return nil, ErrKeyNotFound()
}

func rangeDataBlocks(startKey, endKey string, index []byte) (startBlock, endBlock uint32) {
	i := 0
	for i < len(index) {
		size := uint8(index[i])
		i++
		indexKey := string(index[i : i+int(size)])
		i += int(size)
		block := binary.LittleEndian.Uint32(index[i : i+4])
		i += 4
		if startKey <= indexKey {
			startBlock = block
		}
		if endKey <= indexKey {
			endBlock = block
			return startBlock, endBlock
		}
	}
	return startBlock, endBlock
}

func findKeysInBlocks(startKey, endKey string, data []byte) (entries []*LSMDataEntry) {
	for i := 0; i < len(data); i += BlockSize {
		block := data[i : i+BlockSize]
		j := 0
		for j < len(block) {
			seqID := binary.LittleEndian.Uint64(block[i : i+8])
			i += 8
			keySize := uint8(block[i])
			i++
			key := string(block[i : i+int(keySize)])
			i += int(keySize)
			valueType := uint8(block[i])
			i++
			valueSize := binary.LittleEndian.Uint16(block[i : i+2])
			i += 2
			value := block[i : i+int(valueSize)]
			i += int(valueSize)

			if startKey <= key && key <= endKey {
				entries = append(entries, &LSMDataEntry{
					seqID:     seqID,
					keySize:   keySize,
					key:       key,
					valueType: valueType,
					valueSize: valueSize,
					value:     value,
				})
			} else if key > endKey {
				return entries
			}
		}
	}
	return entries
}
