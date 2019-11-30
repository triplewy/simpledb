package db

import (
	"encoding/binary"
	"os"
)

func createkeyRangeEntry(kr *keyRange) []byte {
	data := []byte{}
	startKeySize := uint8(len(kr.startKey))
	endKeySize := uint8(len(kr.endKey))
	data = append(data, startKeySize)
	data = append(data, []byte(kr.startKey)...)
	data = append(data, endKeySize)
	data = append(data, []byte(kr.endKey)...)
	return data
}

func parsekeyRangeEntry(data []byte) *keyRange {
	i := 0
	startKeySize := uint8(data[i])
	i++
	startKey := string(data[i : i+int(startKeySize)])
	i += int(startKeySize)
	endKeySize := uint8(data[i])
	i++
	endKey := string(data[i : i+int(endKeySize)])
	return &keyRange{
		startKey: startKey,
		endKey:   endKey,
	}
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

	header := append(dataSizeBytes, indexSizeBytes...)
	header = append(header, bloomSizeBytes...)
	header = append(header, keyRangeSizeBytes...)
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
