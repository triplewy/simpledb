package db

import (
	"math"
	"path/filepath"
	"strings"
	"sync"
)

// LSM is struct for all levels in an LSM
type LSM struct {
	levels []*Level
}

// NewLSM instatiates all levels for a new LSM tree
func NewLSM(directory string) (*LSM, error) {
	levels := []*Level{}

	for i := 0; i < 7; i++ {
		var blockCapacity int
		if i == 0 {
			blockCapacity = 2 * 1024
		} else {
			blockCapacity = int(math.Pow10(i)) * multiplier
		}

		level, err := NewLevel(i, blockCapacity, directory)
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

	return &LSM{levels: levels}, nil
}

// Append takes data blocks, an index block, and a key range as input and writes an SST File to level 0.
// It then adds this new file to level 0's manifest
func (lsm *LSM) Append(blocks, index []byte, bloom *Bloom, keyRange *KeyRange) error {
	level := lsm.levels[0]
	fileID := level.getUniqueID()
	filename := filepath.Join(level.directory, fileID+".sst")

	keyRangeEntry := createKeyRangeEntry(keyRange)
	header := createHeader(len(blocks), len(index), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(blocks, index...), bloom.bits...), keyRangeEntry...)...)

	err := writeNewFile(filename, data)
	if err != nil {
		return err
	}

	level.NewSSTFile(fileID, keyRange, bloom)

	return nil
}

// Find is a recursive function that goes through each level of the LSM tree and
// returns if a result is found for the given key. If no result is found, Find throws an error
func (lsm *LSM) Find(key string, levelNum int) (*LSMDataEntry, error) {
	if levelNum > 6 {
		return nil, ErrKeyNotFound()
	}

	level := lsm.levels[levelNum]

	filenames := level.FindSSTFile(key)
	if len(filenames) == 0 {
		return lsm.Find(key, levelNum+1)
	}

	replyChan := make(chan *LSMDataEntry)
	errChan := make(chan error)

	replies := []*LSMDataEntry{}

	var wg sync.WaitGroup
	var errs []error

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go fileFind(filename, key, replyChan, errChan)
	}

	go func() {
		for {
			select {
			case reply := <-replyChan:
				replies = append(replies, reply)
				wg.Done()
			case err := <-errChan:
				errs = append(errs, err)
				wg.Done()
			}
		}
	}()

	wg.Wait()

	if len(replies) > 0 {
		if len(replies) > 1 {
			latestUpdate := replies[0]
			for i := 1; i < len(replies); i++ {
				if replies[i].seqID > latestUpdate.seqID {
					latestUpdate = replies[i]
				}
			}
			return latestUpdate, nil
		}
		return replies[0], nil
	}

	for _, err := range errs {
		if _, ok := err.(*errKeyNotFound); ok {
			continue
		} else if strings.HasSuffix(err.Error(), "no such file or directory") {
			continue
		} else {
			return nil, err
		}
	}

	return lsm.Find(key, levelNum+1)
}

// Range concurrently finds all keys in the LSM tree that fall within the range query.
// Concurrency is achieved by going through each level on its own goroutine
func (lsm *LSM) Range(startKey, endKey string) ([]*LSMDataEntry, error) {
	replyChan := make(chan []*LSMDataEntry)
	errChan := make(chan error)
	result := []*LSMDataEntry{}

	var wg sync.WaitGroup
	var errs []error

	wg.Add(7)
	for _, level := range lsm.levels {
		go level.Range(startKey, endKey, replyChan, errChan)
	}

	go func() {
		for {
			select {
			case reply := <-replyChan:
				result = append(result, reply...)
				wg.Done()
			case err := <-errChan:
				errs = append(errs, err)
				wg.Done()
			}
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return result, errs[0]
	}

	// resultMap := make(map[string]*LSMDataEntry)

	// for _, entry := range all {
	// 	if value, ok := resultMap[entry.key]; !ok {
	// 		resultMap[entry.key] = entry
	// 	} else {
	// 		if value.seqID < entry.seqID {
	// 			resultMap[entry.key] = entry
	// 		}
	// 	}
	// }

	// result := []*LSMDataEntry{}
	// for _, entry := range resultMap {

	// }
	return result, nil
}

// Close closes all levels in the LSM
func (lsm *LSM) Close() {
	for _, level := range lsm.levels {
		level.Close()
	}
}
