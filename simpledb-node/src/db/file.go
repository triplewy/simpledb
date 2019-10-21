package db

import (
	"encoding/binary"
	"errors"
	"os"
)

func fileFind(filename, key string, replyChan chan *LSMFind) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	dataSize, indexSize, err := readHeader(f)
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, 16+int64(dataSize))
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	if numBytes != int(indexSize) {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    errors.New("Did not read correct amount of bytes for index"),
		}
		return
	}

	blockIndex := findDataBlock(key, index)

	block := make([]byte, blockSize)
	numBytes, err = f.ReadAt(block, 16+int64(blockSize*int(blockIndex)))
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}
	if numBytes != blockSize {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    errors.New("Did not read correct amount of bytes for data block"),
		}
		return
	}

	offset, size, err := findKeyInBlock(key, block)
	replyChan <- &LSMFind{
		offset: offset,
		size:   size,
		err:    err,
	}
}

func fileRangeQuery(filename, startKey, endKey string, replyChan chan *LSMRange) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		replyChan <- &LSMRange{
			lsmFinds: nil,
			err:      err,
		}
		return
	}

	dataSize, indexSize, err := readHeader(f)
	if err != nil {
		replyChan <- &LSMRange{
			lsmFinds: nil,
			err:      err,
		}
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, 16+int64(dataSize))
	if err != nil {
		replyChan <- &LSMRange{
			lsmFinds: nil,
			err:      err,
		}
		return
	}
	if numBytes != int(indexSize) {
		replyChan <- &LSMRange{
			lsmFinds: nil,
			err:      errors.New("Did not read correct amount of bytes for index"),
		}
		return
	}

	startBlock, endBlock := rangeDataBlocks(startKey, endKey, index)

	size := int(endBlock+1)*blockSize - int(startBlock)*blockSize
	blocks := make([]byte, size)

	numBytes, err = f.ReadAt(blocks, 16+int64(blockSize*int(startBlock)))
	if err != nil {
		replyChan <- &LSMRange{
			lsmFinds: nil,
			err:      err,
		}
		return
	}
	if numBytes != size {
		replyChan <- &LSMRange{
			lsmFinds: nil,
			err:      errors.New("Did not read correct amount of bytes for data block"),
		}
		return
	}

	lsmFinds, err := findKeysInBlocks(startKey, endKey, blocks)
	replyChan <- &LSMRange{
		lsmFinds: lsmFinds,
		err:      err,
	}
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

func findKeyInBlock(key string, block []byte) (offset uint64, size uint32, err error) {
	i := 0

	for i < blockSize {
		size := uint8(block[i])
		i++
		foundKey := string(block[i : i+int(size)])
		i += int(size)
		if key == foundKey {
			valueOffset := binary.LittleEndian.Uint64(block[i : i+8])
			i += 8
			valueSize := binary.LittleEndian.Uint32(block[i : i+4])
			i += 4
			return valueOffset, valueSize, nil
		}
		i += 12
	}

	return 0, 0, errors.New("Key not found")
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

func findKeysInBlocks(startKey, endKey string, data []byte) ([]*LSMFind, error) {
	result := []*LSMFind{}

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
				offset := binary.LittleEndian.Uint64(block[j : j+8])
				j += 8
				size := binary.LittleEndian.Uint32(block[j : j+4])
				j += 4
				result = append(result, &LSMFind{
					offset: offset,
					size:   size,
					err:    nil,
				})
			} else {
				if len(result) == 0 {
					return nil, errors.New("No keys found")
				}
				return result, nil
			}
		}
	}

	if len(result) == 0 {
		return nil, errors.New("No keys found")
	}
	return result, nil
}
