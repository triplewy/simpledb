package db

import (
	"errors"
	"math"
	"os"
	"strings"
	"sync"
)

type LSM struct {
	levels []*Level
}

type LSMFind struct {
	offset uint64
	size   uint32
	err    error
}

type LSMRange struct {
	lsmFinds []*LSMFind
	err      error
}

func NewLSM() (*LSM, error) {
	levels := []*Level{}

	for i := 0; i < 7; i++ {
		var blockCapacity int
		if i == 0 {
			blockCapacity = 2 * 1024
		} else {
			blockCapacity = int(math.Pow10(i)) * multiplier
		}

		level := NewLevel(i, blockCapacity)

		if i > 0 {
			above := levels[i-1]
			above.below = level
			level.above = above
		}
		levels = append(levels, level)
	}

	return &LSM{levels: levels}, nil
}

func (lsm *LSM) Append(blocks, index []byte, startKey, endKey string) error {
	var f *os.File
	var err error

	level := lsm.levels[0]
	filename := level.getUniqueID()

	f, err = os.OpenFile(level.directory+filename+".sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	header := createHeader(len(blocks), len(index))
	data := append(header, append(blocks, index...)...)

	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return errors.New("Num bytes written to SST does not match size of data")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	level.NewSSTFile(filename, startKey, endKey)

	return nil
}

func (lsm *LSM) Find(key string, levelNum int) (*LSMFind, error) {
	if levelNum > 6 {
		return nil, errors.New("Key not found")
	}

	level := lsm.levels[levelNum]

	filenames := level.FindSSTFile(key)
	if len(filenames) == 0 {
		return lsm.Find(key, levelNum+1)
	}

	replyChan := make(chan *LSMFind)
	replies := []*LSMFind{}

	var wg sync.WaitGroup
	var errs []error

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go func(filename string) {
			fileFind(filename, key, replyChan)
		}(filename)
	}

	go func() {
		for reply := range replyChan {
			if reply.err != nil {
				errs = append(errs, reply.err)
			} else {
				replies = append(replies, reply)
			}
			wg.Done()
		}
	}()

	wg.Wait()

	if len(replies) > 0 {
		if len(replies) > 1 {
			latestUpdate := replies[0]
			for i := 1; i < len(replies); i++ {
				if replies[i].offset > latestUpdate.offset {
					latestUpdate = replies[i]
				}
			}
			return latestUpdate, nil
		}
		return replies[0], nil
	}

	for _, err := range errs {
		if !(strings.HasSuffix(err.Error(), "no such file or directory") || err.Error() == "Key not found") {
			return nil, err
		}
	}

	return lsm.Find(key, levelNum+1)
}

func (lsm *LSM) Range(startKey, endKey string) ([]*LSMFind, error) {
	replyChan := make(chan *LSMRange)
	var wg sync.WaitGroup
	var errs []error

	wg.Add(7)
	for _, level := range lsm.levels {
		go func(level *Level) {
			level.Range(startKey, endKey, replyChan)
		}(level)
	}

	result := []*LSMFind{}
	go func() {
		for reply := range replyChan {
			if reply.err != nil {
				errs = append(errs, reply.err)
			} else {
				result = append(result, reply.lsmFinds...)
			}
			wg.Done()
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return result, errs[0]
	}

	return result, nil
}
