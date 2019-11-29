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

func asyncUpdates(db *DB, entries []*KV, memoryKV map[string]string) error {
	type update struct {
		key   string
		value string
		err   error
	}
	var wg sync.WaitGroup
	updateChan := make(chan *update)
	wg.Add(len(entries))

	startTime := time.Now()
	for _, kv := range entries {
		go func(key string, value interface{}) {
			err := db.Update(func(txn *Txn) error {
				txn.Write(key, value)
				return nil
			})
			if value == nil {
				value = ""
			}
			updateChan <- &update{
				key:   key,
				value: value.(string),
				err:   err,
			}
		}(kv.key, kv.value)
	}

	errs := make(map[string]int)
	go func() {
		for {
			select {
			case update := <-updateChan:
				err := update.err
				if err != nil {
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				} else {
					memoryKV[update.key] = update.value
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

func asyncDeletes(db *DB, keys []string, memoryKV map[string]string) error {
	type update struct {
		key string
		err error
	}
	var wg sync.WaitGroup
	updateChan := make(chan *update)
	wg.Add(len(keys))
	startTime := time.Now()
	for _, key := range keys {
		go func(key string) {
			err := db.Update(func(txn *Txn) error {
				txn.Delete(key)
				return nil
			})
			updateChan <- &update{
				key: key,
				err: err,
			}
		}(key)
	}

	errs := make(map[string]int)
	go func() {
		for {
			select {
			case update := <-updateChan:
				err := update.err
				if err != nil {
					if _, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()]++
					}
				} else {
					memoryKV[update.key] = ""
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Duration deleting %d items: %v\n", len(keys), duration)

	if len(errs) > 0 {
		fmt.Printf("Encountered errors during put: %v\n", errs)
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
						// fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, "Key not found")
						return err
					}
					return nil
				}
				if result.(string) != value {
					if value == "__delete__" {
						// fmt.Printf("Key: %v, Expected: Key not found, Got: %v\n", key, result.(string))
						return errors.New("Key should have been deleted")
					}
					// fmt.Printf("Key: %v, Expected: %v, Got: %v\n", key, value, result.(string))
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
