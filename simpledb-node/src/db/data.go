package db

import (
	"encoding/binary"
	"os"
)

func createKeyRangeEntry(keyRange *KeyRange) []byte {
	data := []byte{}

	startKeySize := uint8(len(keyRange.startKey))
	endKeySize := uint8(len(keyRange.endKey))

	data = append(data, startKeySize)
	data = append(data, []byte(keyRange.startKey)...)
	data = append(data, endKeySize)
	data = append(data, []byte(keyRange.endKey)...)
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
		return 0, 0, 0, 0, ErrReadUnexpectedBytes("Header")
	}

	dataSize = binary.LittleEndian.Uint64(header[:8])
	indexSize = binary.LittleEndian.Uint64(header[8:16])
	bloomSize = binary.LittleEndian.Uint64(header[16:24])
	keyRangeSize = binary.LittleEndian.Uint64(header[24:])

	return dataSize, indexSize, bloomSize, keyRangeSize, nil
}
