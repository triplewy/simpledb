package db

import (
	"encoding/binary"
	"errors"
	"os"
)

func createVlogEntry(key, value string) ([]byte, error) {
	keySize := uint8(len(key))
	valueSize := uint16(len(value))

	valueSizeBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(valueSizeBytes, valueSize)

	dataSize := 3 + len(key) + len(value)
	data := make([]byte, dataSize)

	i := 0
	i += copy(data[i:], []byte{keySize})
	i += copy(data[i:], key)
	i += copy(data[i:], valueSizeBytes)
	i += copy(data[i:], value)

	if i != dataSize {
		return nil, errors.New("Expected length of data array does not match actual length")
	}
	return data, nil
}

func createLsmEntry(key string, offset uint64, size uint32) []byte {
	lsmEntry := make([]byte, 13+len(key))

	offsetBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBytes, offset)

	dataSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataSizeBytes, size)

	copy(lsmEntry[0:], []byte{uint8(len(key))})
	copy(lsmEntry[1:], key)
	copy(lsmEntry[1+len(key):], offsetBytes)
	copy(lsmEntry[1+len(key)+8:], dataSizeBytes)

	return lsmEntry
}

func createLsmIndex(key string, block uint32) []byte {
	indexEntry := make([]byte, 5+len(key))

	blockBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockBytes, block)

	copy(indexEntry[0:], []byte{uint8(len(key))})
	copy(indexEntry[1:], key)
	copy(indexEntry[1+len(key):], blockBytes)

	return indexEntry
}

func createKeyRangeEntry(startKey, endKey string) []byte {
	data := []byte{}

	startKeySize := uint8(len(startKey))
	endKeySize := uint8(len(endKey))

	data = append(data, startKeySize)
	data = append(data, []byte(startKey)...)
	data = append(data, endKeySize)
	data = append(data, []byte(endKey)...)
	return data
}

func createHeader(dataSize, indexSize, bloomSize, keyRangeSize int) []byte {
	dataSizeBytes := make([]byte, 8)
	indexSizeBytes := make([]byte, 8)
	bloomSizeBytes := make([]byte, 8)
	keyRangeSizeBytes := make([]byte, 8)

	binary.LittleEndian.PutUint64(dataSizeBytes, uint64(dataSize))
	binary.LittleEndian.PutUint64(indexSizeBytes, uint64(indexSize))
	binary.LittleEndian.PutUint64(bloomSizeBytes, uint64(bloomSize))
	binary.LittleEndian.PutUint64(keyRangeSizeBytes, uint64(keyRangeSize))

	header := append(append(append(dataSizeBytes, indexSizeBytes...), bloomSizeBytes...), keyRangeSizeBytes...)

	return header
}

func readHeader(f *os.File) (dataSize, indexSize, bloomSize, keyRangeSize uint64, err error) {
	header := make([]byte, headerSize)

	numBytes, err := f.Read(header)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if numBytes != len(header) {
		return 0, 0, 0, 0, newErrReadUnexpectedBytes("Header")
	}

	dataSize = binary.LittleEndian.Uint64(header[:8])
	indexSize = binary.LittleEndian.Uint64(header[8:16])
	bloomSize = binary.LittleEndian.Uint64(header[16:24])
	keyRangeSize = binary.LittleEndian.Uint64(header[24:])

	return dataSize, indexSize, bloomSize, keyRangeSize, nil
}

func appendDataBlock(block, input []byte) (oldBlock, newBlock []byte) {
	var appendBlock []byte
	createdNewBlock := false

	keySize := uint8(input[0])
	key := string(input[1 : 1+keySize])
	offset := binary.LittleEndian.Uint64(input[1+keySize : 1+keySize+8])
	size := binary.LittleEndian.Uint32(input[1+keySize+8 : 1+keySize+8+4])

	entry := createLsmEntry(key, offset, size)

	if len(block)+len(entry) > blockSize {
		appendBlock = []byte{}
		createdNewBlock = true
	} else {
		appendBlock = block
	}

	appendBlock = append(appendBlock, entry...)

	if createdNewBlock {
		return block, appendBlock
	}
	return appendBlock, nil
}
