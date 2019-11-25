package db

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func asyncUpdates(db *DB, entries []*KV) error {
	var wg sync.WaitGroup
	errChan := make(chan error)
	wg.Add(len(entries))

	startTime := time.Now()
	for _, kv := range entries {
		go func(key string, value interface{}) {
			err := db.Update(func(txn *Txn) error {
				txn.Write(key, value)
				return nil
			})
			errChan <- err
		}(kv.key, kv.value)
	}

	errs := make(map[string]int)
	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Duration inserting %d items: %v\n", len(entries), duration)

	if len(errs) > 0 {
		fmt.Printf("Encountered errors during put: %v\n", errs)
		return errors.New("Async Updates failed")
	}
	return nil
}

func asyncPuts(db *DB, keys []string, values []interface{}) error {
	if len(keys) != len(values) {
		return errors.New("Length of keys does not match length of values")
	}
	var wg sync.WaitGroup
	errChan := make(chan error)
	wg.Add(len(keys))

	startTime := time.Now()
	for i, key := range keys {
		go func(key string, value interface{}) {
			errChan <- db.Put(key, value)
		}(key, values[i])
	}

	errs := make(map[string]int)
	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Duration inserting %d items: %v\n", len(keys), duration)

	if len(errs) > 0 {
		fmt.Printf("Encountered errors during put: %v\n", errs)
		return errors.New("Async Puts failed")
	}
	return nil
}

func syncPuts(db *DB, keys []string, values []interface{}) error {
	if len(keys) != len(values) {
		return errors.New("Length of keys does not match length of values")
	}

	startTime := time.Now()
	for i, key := range keys {
		err := db.Put(key, values[i])
		if err != nil {
			return err
		}
	}
	duration := time.Since(startTime)
	fmt.Printf("Duration inserting %d items: %v\n", len(keys), duration)
	return nil
}

func asyncDeletes(db *DB, keys []string) error {
	var wg sync.WaitGroup
	errChan := make(chan error)

	startTime := time.Now()
	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			err := db.Delete(key)
			errChan <- err
		}(key)
	}

	errs := make(map[string]int)
	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					if count, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()] = count + 1
					}
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Duration deleting %d items: %v\n", len(keys), duration)

	if len(errs) > 0 {
		fmt.Printf("Encountered errors during delete: %v\n", errs)
		return errors.New("Async Deletes failed")
	}
	return nil
}

func asyncViews(db *DB, keys []string, memoryKV map[string]string) error {
	var wg sync.WaitGroup
	errChan := make(chan error)

	startTime := time.Now()
	for _, key := range keys {
		wg.Add(1)
		value := memoryKV[key]
		go func(key, value string) {
			err := db.View(func(txn *Txn) error {
				result, err := txn.Read(key)
				if err != nil {
					if err.Error() == "Key not found" && !(value == "__delete__" || value == "") {
						fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, "Key not found")
						return err
					}
					return nil
				}
				if result.(string) != value {
					if value == "__delete__" {
						fmt.Printf("Key: %v, Expected: Key not found, Got: %v\n", key, result.(string))
						return errors.New("Key should have been deleted")
					}
					fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, result.(string))
					return errors.New("Incorrect result for get")
				}
				return nil
			})
			errChan <- err
		}(key, value)
	}

	wrong := 0
	errs := make(map[string]int)

	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					wrong++
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Duration reading %d items: %v\n", len(keys), duration)

	fmt.Printf("Correct: %f%%\n", float64(len(keys)-wrong)/float64(len(keys))*float64(100))
	if len(errs) > 0 {
		fmt.Printf("Encountered errors during read: %v\n", errs)
		return errors.New("Async get failed")
	}
	return nil
}

func asyncGets(db *DB, keys []string, memoryKV map[string]string) error {
	var wg sync.WaitGroup
	errChan := make(chan error)

	startTime := time.Now()
	for _, key := range keys {
		wg.Add(1)
		value := memoryKV[key]
		go func(key, value string) {
			result, err := db.Get(key)
			if err != nil {
				if err.Error() == "Key not found" && !(value == "__delete__" || value == "") {
					fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, "Key not found")
					errChan <- err
				} else {
					errChan <- nil
				}
			} else {
				if result.value.(string) != value {
					if value == "__delete__" {
						fmt.Printf("Key: %v, Expected: Key not found, Got: %v\n", key, result.value.(string))
						errChan <- errors.New("Key should have been deleted")
					} else {
						fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, result.value.(string))
						errChan <- errors.New("Incorrect result for get")
					}
				} else {
					errChan <- nil
				}
			}
		}(key, value)
	}

	wrong := 0
	errs := make(map[string]int)

	go func() {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					wrong++
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Duration reading %d items: %v\n", len(keys), duration)

	fmt.Printf("Correct: %f%%\n", float64(len(keys)-wrong)/float64(len(keys))*float64(100))
	if len(errs) > 0 {
		fmt.Printf("Encountered errors during read: %v\n", errs)
		return errors.New("Async get failed")
	}
	return nil
}

func syncGets(db *DB, keys []string, memoryKV map[string]string) error {
	wrong := 0
	errs := make(map[string]int)
	errKeys := []string{}
	startTime := time.Now()
	for _, key := range keys {
		value := memoryKV[key]
		result, err := db.Get(key)
		if err != nil {
			if !((value == "" || value == "__delete__") && err.Error() == "Key not found") {
				errKeys = append(errKeys, key)
				fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, "Key not found")
				wrong++
				if count, ok := errs[err.Error()]; !ok {
					errs[err.Error()] = 1
				} else {
					errs[err.Error()] = count + 1
				}
			}
		} else {
			if result.value.(string) != value {
				if value == "__delete__" {
					errKeys = append(errKeys, key)
					err := errors.New("Key should have been deleted")
					wrong++
					if count, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()] = count + 1
					}
				} else {
					errKeys = append(errKeys, key)
					fmt.Printf("Key: %v, Expected: %v, Got: %v\n", result.key, value, result.value.(string))
					err := errors.New("Incorrect result for get")
					wrong++
					if count, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()] = count + 1
					}
				}
			}
		}
	}
	duration := time.Since(startTime)
	fmt.Printf("Duration reading %d items: %v\n", len(keys), duration)

	fmt.Printf("Correct: %f%%\n", float64(len(keys)-wrong)/float64(len(keys))*float64(100))
	if len(errs) > 0 {
		fmt.Printf("Encountered errors during read: %v\n", errs)
		for _, key := range errKeys {
			for _, level := range db.lsm.levels {
				files := level.FindSSTFile(key)
				fmt.Println(key, level.level, files)
				if len(files) > 0 {
					var wg sync.WaitGroup
					replyChan := make(chan *LSMDataEntry)
					errChan := make(chan error)

					go func() {
						for {
							select {
							case entry := <-replyChan:
								kv, err := parseDataEntry(entry)
								if err != nil {
									fmt.Println(err)
								} else if kv.value.(string) != memoryKV[key] {
									fmt.Println(key, kv.value.(string), memoryKV[key])
								} else {
									fmt.Println(key, "Correct result found!")
								}
								wg.Done()
							case err := <-errChan:
								fmt.Println(err)
								wg.Done()
							}
						}
					}()
					for _, file := range files {
						wg.Add(1)
						fileFind(file, key, replyChan, errChan)
					}
					wg.Wait()
				}
			}
		}
		return errors.New("Async get failed")
	}
	return nil
}

func writeEntriesToFile(filename string, entries []*LSMDataEntry) error {
	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		return err
	}

	keyRangeEntry := createKeyRangeEntry(keyRange)
	header := createHeader(len(dataBlocks), len(indexBlock), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(dataBlocks, indexBlock...), bloom.bits...), keyRangeEntry...)...)

	err = writeNewFile(filename, data)
	if err != nil {
		return err
	}
	return nil
}

func checkManifests(levels []*Level) error {
	for i := 0; i < 7; i++ {
		level := levels[i]
		dir := filepath.Join("data", "L"+strconv.Itoa(i))
		d, err := ioutil.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, fileInfo := range d {
			file := fileInfo.Name()
			fileID := strings.Split(file, ".")[0]
			keyRange, ok := level.manifest[fileID]
			if !ok {
				return errors.New("File in directory is not in manifest")
			}
			fmt.Println("Level:", i, keyRange)
		}
	}
	return nil
}
