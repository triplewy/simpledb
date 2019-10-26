package db

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// LSM is struct for all levels in an LSM
type LSM struct {
	levels []*Level
}

// LSMDataEntry is struct that represents an entry into an LSM Data Block
type LSMDataEntry struct {
	keySize    uint8
	key        string
	vlogOffset uint64
	vlogSize   uint32
}

// LSMIndexEntry is struct that represents an entry into an LSM Index Block
type LSMIndexEntry struct {
	keySize uint8
	key     string
	block   uint32
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
func (lsm *LSM) Append(blocks, index []byte, bloom *Bloom, startKey, endKey string) error {
	level := lsm.levels[0]
	filename := level.getUniqueID()

	f, err := os.OpenFile(filepath.Join(level.directory, filename+".sst"), os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	header := createHeader(len(blocks), len(index), len(bloom.bits))
	data := append(header, append(append(blocks, index...), bloom.bits...)...)

	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return newErrWriteUnexpectedBytes("SST File")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	level.NewSSTFile(filename, startKey, endKey, bloom)

	return nil
}

// Find is a recursive function that goes through each level of the LSM tree and
// returns if a result is found for the given key. If no result is found, Find throws an error
func (lsm *LSM) Find(key string, levelNum int) (*LSMDataEntry, error) {
	if levelNum > 6 {
		return nil, newErrKeyNotFound()
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
				if replies[i].vlogOffset > latestUpdate.vlogOffset {
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

	return result, nil
}
