package db

import (
	"errors"
	"os"
)

func mergeAbove(above []string) ([][]byte, error) {
	if len(above) > 1 {
		mid := len(above) / 2

		left, err := mergeAbove(above[:mid])
		if err != nil {
			return nil, err
		}
		right, err := mergeAbove(above[mid:])
		if err != nil {
			return nil, err
		}

		result := mergeHelper(left, right)

		return result, nil
	}

	return mmap(above[0])
}

func mergeHelper(left, right [][]byte) [][]byte {
	od := newOrderedDict()
	i, j := 0, 0

	for i < len(left) && j < len(right) {
		leftEntry := newODValue(left[i])
		rightEntry := newODValue(right[j])

		if val, ok := od.Get(leftEntry.Key()); ok {
			if val.(odValue).Offset() < leftEntry.Offset() {
				od.Set(leftEntry.Key(), leftEntry)
			}
			i++
			continue
		}

		if val, ok := od.Get(rightEntry.Key()); ok {
			if val.(odValue).Offset() < rightEntry.Offset() {
				od.Set(rightEntry.Key(), rightEntry)
			}
			j++
			continue
		}

		if leftEntry.Key() < rightEntry.Key() {
			od.Set(leftEntry.Key(), leftEntry)
			i++
		} else if leftEntry.Key() == rightEntry.Key() {
			if leftEntry.Offset() > rightEntry.Offset() {
				od.Set(leftEntry.Key(), leftEntry)
			} else {
				od.Set(rightEntry.Key(), rightEntry)
			}
			i++
			j++
		} else {
			od.Set(rightEntry.Key(), rightEntry)
			j++
		}
	}

	for i < len(left) {
		leftEntry := newODValue(left[i])
		od.Set(leftEntry.Key(), leftEntry)
		i++
	}

	for j < len(right) {
		rightEntry := newODValue(right[j])
		od.Set(rightEntry.Key(), rightEntry)
		j++
	}

	result := [][]byte{}

	for val := range od.Iterate() {
		result = append(result, val.(odValue).Entry())
	}

	return result
}

func mmap(filename string) ([][]byte, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	dataSize, _, err := readHeader(f)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, dataSize)

	numBytes, err := f.ReadAt(buffer, 16)
	if err != nil {
		return nil, err
	}

	if numBytes != len(buffer) {
		return nil, errors.New("Num bytes read from file does not match expected data block size")
	}

	keys := [][]byte{}

	for i := 0; i < len(buffer); i += blockSize {
		block := buffer[i : i+blockSize]
		j := 0
		for j < len(block) && block[j] != byte(0) {
			keySize := uint8(block[j])
			j++
			entry := make([]byte, 13+int(keySize))
			copy(entry[0:], []byte{keySize})
			copy(entry[1:], block[j:j+int(keySize)+12])
			j += int(keySize) + 12
			keys = append(keys, entry)
		}
	}

	return keys, nil
}
