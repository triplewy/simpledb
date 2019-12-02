package db

import (
	"fmt"
	"path/filepath"
	"sync"
)

// LSM is struct for all levels in an LSM
type lsm struct {
	levels []*level
	fm     *fileManager
}

// newLSM instatiates all levels for a new LSM tree
func newLSM(directory string) (*lsm, error) {
	fm := newFileManager()
	levels := []*level{}
	for i := 0; i < 7; i++ {
		level, err := newLevel(i, directory, fm)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			above := levels[i-1]
			above.below = level
			level.above = above
		}
		levels = append(levels, level)
	}

	return &lsm{
		levels: levels,
		fm:     fm,
	}, nil
}

// Write takes data blocks, an index block, and a key range as input and writes an SST File to level 0.
// It then adds this new file to level 0's manifest
func (lsm *lsm) Write(blocks, index []byte, bloom *bloom, keyRange *keyRange) error {
	level := lsm.levels[0]
	fileID := level.getUniqueID()
	filename := filepath.Join(level.directory, fileID+".sst")

	keyRangeEntry := createkeyRangeEntry(keyRange)
	header := createHeader(len(blocks), len(index), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(blocks, index...), bloom.bits...), keyRangeEntry...)...)

	err := lsm.fm.Write(filename, data)
	if err != nil {
		return err
	}

	level.NewSSTFile(fileID, keyRange, bloom)

	return nil
}

// Read goes through each level of the LSM tree and returns if a result is found for the given key.
// If no result is found, Find throws a KeyNotFound error
func (lsm *lsm) Read(key string, ts uint64) (*Entry, error) {
	for _, level := range lsm.levels {
		entry, err := level.Find(key, ts)
		if err != nil {
			switch err.(type) {
			case *ErrKeyNotFound:
				continue
			default:
				return nil, err
			}
		} else {
			return entry, nil
		}
	}
	return nil, newErrKeyNotFound()
}

// Scan concurrently finds all keys in the LSM tree that fall within the range query.
// Concurrency is achieved by going through each level on its own goroutine
func (lsm *lsm) Scan(keyRange *keyRange, ts uint64) ([]*Entry, error) {
	replyChan := make(chan []*Entry)
	errChan := make(chan error)
	result := []*Entry{}
	errs := make(map[string]int)
	var wg sync.WaitGroup

	wg.Add(7)
	for _, lvl := range lsm.levels {
		go func(level *level) {
			entries, err := level.Range(keyRange, ts)
			if err != nil {
				errChan <- err
			} else {
				replyChan <- entries
			}
		}(lvl)
	}

	go func() {
		for {
			select {
			case reply := <-replyChan:
				result = append(result, reply...)
				wg.Done()
			case err := <-errChan:
				if _, ok := errs[err.Error()]; !ok {
					errs[err.Error()] = 1
				} else {
					errs[err.Error()]++
				}
				wg.Done()
			}
		}
	}()
	wg.Wait()

	if len(errs) > 0 {
		return result, fmt.Errorf("Errors during LSM range query: %v", errs)
	}
	return result, nil
}

// Close closes all levels in the LSM
func (lsm *lsm) Close() {
	for _, level := range lsm.levels {
		level.Close()
	}
}
