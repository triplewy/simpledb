package db

import (
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
)

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func floor(x float64) (int64, error) {
	f64 := math.Floor(x)
	if f64 >= math.MaxInt64 || f64 <= math.MinInt64 {
		return 0, errors.New("input is out of int64 range")
	}

	return int64(f64), nil
}

// DeleteData deletes all data from database
func DeleteData() error {
	dirs := []string{"data/L0", "data/L1", "data/L2", "data/L3", "data/L4", "data/L5", "data/L6", "data/VLog"}

	for _, dir := range dirs {
		d, err := ioutil.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, f := range d {
			os.RemoveAll(path.Join([]string{dir, f.Name()}...))
		}
	}

	return nil
}

func populateSSTFile(keys []string, filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	data := []byte{}
	block := []byte{}

	for _, key := range keys {
		input := []byte{uint8(len(key))}
		input = append(input, []byte(key)...)
		input = append(input, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
		block, newBlock := appendDataBlock(block, input)
		if newBlock != nil {
			data = append(data, block...)
			block = newBlock
		}
	}

	header := createHeader(len(data), 0)

	numBytes, err := f.Write(append(header, data...))
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return err
	}

	return nil
}

func readAllSSTs() ([]map[string]bool, error) {
	result := []map[string]bool{}
	for i := 0; i < 7; i++ {
		result = append(result, make(map[string]bool))
	}

	for i := 0; i < 7; i++ {
		dir := "data/L" + strconv.Itoa(i)
		d, err := ioutil.ReadDir(dir)
		if err != nil {
			return nil, err
		}

		for _, fileInfo := range d {
			if fileInfo.Name() != "manifest" {
				keys, err := readAllKeys(path.Join([]string{dir, fileInfo.Name()}...))
				if err != nil {
					return nil, err
				}
				for _, key := range keys {
					result[i][key] = true
				}
			}
		}
	}

	return result, nil
}

func readAllKeys(filename string) ([]string, error) {
	values, err := mmap(filename)
	if err != nil {
		return nil, err
	}

	result := make([]string, len(values))

	for i, val := range values {
		keySize := uint8(val[0])
		result[i] = string(val[1 : 1+keySize])
	}

	return result, nil
}
