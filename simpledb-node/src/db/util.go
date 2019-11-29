package db

import (
	"io/ioutil"
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

// DeleteData deletes all data from database
func DeleteData() error {
	dirs := []string{"data/L0", "data/L1", "data/L2", "data/L3", "data/L4", "data/L5", "data/L6", "data/memtables"}

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
	entries, err := mmap(filename)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(entries))

	for i, entry := range entries {
		result[i] = entry.key
	}
	return result, nil
}
