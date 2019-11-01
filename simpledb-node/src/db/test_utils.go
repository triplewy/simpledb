package db

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

func asyncPuts(db *DB, keys []string, values []interface{}) error {
	if len(keys) != len(values) {
		return errors.New("Length of keys does not match length of values")
	}
	var wg sync.WaitGroup
	errChan := make(chan error)

	startTime := time.Now()
	for i, key := range keys {
		wg.Add(1)
		go func(key string, value interface{}) {
			err := db.Put(key, value)
			errChan <- err
		}(key, values[i])
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
	fmt.Printf("Duration inserting %d items: %v\n", len(keys), duration)

	if len(errs) > 0 {
		fmt.Printf("Encountered errors during put: %v\n", errs)
		return errors.New("Async Puts failed")
	}
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
				if !(value == "__delete__" && err.Error() == "Key not found") {
					fmt.Printf("Expected: %v, Got: %v\n", value, "Key not found")
					errChan <- err
				} else {
					errChan <- nil
				}
			} else {
				if result != value {
					if value == "__delete__" {
						errChan <- errors.New("Key should have been deleted")
					} else {
						fmt.Printf("Expected: %v, Got: %v\n", value, result)
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
	startTime := time.Now()
	for _, key := range keys {
		value := memoryKV[key]
		result, err := db.Get(key)
		if err != nil {
			if !(value == "__delete__" && err.Error() == "Key not found") {
				fmt.Printf("Expected: %v, Got: %v\n", value, "Key not found")
				wrong++
				if count, ok := errs[err.Error()]; !ok {
					errs[err.Error()] = 1
				} else {
					errs[err.Error()] = count + 1
				}
			}
		} else {
			if result != value {
				if value == "__delete__" {
					err := errors.New("Key should have been deleted")
					wrong++
					if count, ok := errs[err.Error()]; !ok {
						errs[err.Error()] = 1
					} else {
						errs[err.Error()] = count + 1
					}
				} else {
					fmt.Printf("Expected: %v, Got: %v\n", value, result)
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
		return errors.New("Async get failed")
	}
	return nil
}
