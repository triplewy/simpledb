package db

import (
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path"
)

// Max returns the larger of x or y.
func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func Floor(x float64) (int64, error) {
	f64 := math.Floor(x)
	if f64 >= math.MaxInt64 || f64 <= math.MinInt64 {
		return 0, errors.New("input is out of int64 range")
	}

	return int64(f64), nil
}

func DeleteData() error {
	dirs := []string{"data/L0", "data/L1", "data/L2", "data/L3", "data/L4", "data/L5", "data/L6"}

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

func PopulateSSTFile(keys []string, filename string) error {
	indexBlock := make([]byte, 2*1024/blockSize*(3+keySize))

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	for _, key := range keys {
		keySize := uint8(len(key))
		entry := make([]byte, keySize+13)
		copy(entry[0:], []byte{keySize})
		copy(entry[1:], []byte(key))

		numBytes, err := f.Write(entry)
		if err != nil {
			return err
		}
		if numBytes != len(entry) {
			return err
		}
	}

	numBytes, err := f.Write(indexBlock)
	if err != nil {
		return err
	}
	if numBytes != len(indexBlock) {
		return err
	}

	return nil
}
