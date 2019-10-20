package db

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type keyRange struct {
	startKey string
	endKey   string
}

type merge struct {
	aboveFiles []string
	belowFile  string
	keyRange   *keyRange
}

type compactReply struct {
	mergedFiles []string
	err         error
}

// Level represents struct for level in LSM tree
type Level struct {
	level     int
	capacity  int
	size      int
	directory string

	merging   map[string]struct{}
	mergeLock sync.RWMutex

	manifest     map[string]*keyRange
	manifestSync map[string]*keyRange
	manifestLock sync.RWMutex

	compactReqChan   chan []*merge
	compactReplyChan chan *compactReply

	above *Level
	below *Level
}

// NewLevel creates a new level in the LSM tree
func NewLevel(level, capacity int) *Level {
	l := &Level{
		level:     level,
		capacity:  capacity,
		size:      0,
		directory: "data/L" + strconv.Itoa(level) + "/",

		merging: make(map[string]struct{}),

		manifest:     make(map[string]*keyRange),
		manifestSync: make(map[string]*keyRange),

		compactReqChan:   make(chan []*merge, 16),
		compactReplyChan: make(chan *compactReply, 16),

		above: nil,
		below: nil,
	}

	go l.run()

	return l
}

func (level *Level) getUniqueID() string {
	level.manifestLock.RLock()
	defer level.manifestLock.RUnlock()

	id := uuid.New().String()[:8]
	if _, ok := level.manifest[id]; !ok {
		return id
	}
	return level.getUniqueID()
}

// Merge takes an empty or existing file at the current level and merges it with file(s) from the level above
func (level *Level) Merge(below string, above []string) {
	if below == "" && len(above) == 1 {
		info, err := os.Stat(above[0])
		if err != nil {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
			return
		}

		size := int(info.Size())

		aboveArr := strings.Split(above[0], "/")
		aboveID := strings.Split(aboveArr[len(aboveArr)-1], ".")[0]

		fileID := level.getUniqueID()

		err = os.Rename(above[0], level.directory+fileID+".sst")
		if err != nil {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
			return
		}

		level.manifestLock.Lock()
		level.above.manifestLock.Lock()

		level.manifest[fileID] = level.above.manifest[aboveID]
		delete(level.above.manifest, aboveID)

		level.manifestLock.Unlock()
		level.above.manifestLock.Unlock()

		level.size += size
		level.above.compactReplyChan <- &compactReply{mergedFiles: []string{}, err: nil}
		return
	}

	var values [][]byte

	mergedAbove, err := mergeAbove(above)
	if err != nil {
		level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
		return
	}

	if below != "" {
		belowValues, err := mmap(below)
		if err != nil {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
			return
		}

		values = mergeHelper(belowValues, mergedAbove)
	} else {
		values = mergedAbove
	}

	err = level.writeMerge(below, values)
	if err != nil {
		level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
		return
	}

	level.above.compactReplyChan <- &compactReply{mergedFiles: above, err: nil}
}

func (level *Level) writeMerge(below string, values [][]byte) error {
	startItem := values[0]
	startKeySize := uint8(startItem[0])
	startKey := string(startItem[1 : 1+startKeySize])

	endItem := values[len(values)-1]
	endKeySize := uint8(endItem[0])
	endKey := string(endItem[1 : 1+endKeySize])

	indexBlock := []byte{}
	dataBlocks := []byte{}
	block := make([]byte, blockSize)
	currBlock := uint32(0)

	i := 0
	for _, item := range values {
		if i+len(item) > blockSize {
			dataBlocks = append(dataBlocks, block...)
			block = make([]byte, blockSize)
			i = 0
		}

		if i == 0 {
			keySize := uint8(item[0])
			key := item[1 : 1+keySize]
			indexEntry := createLsmIndex(string(key), currBlock)
			indexBlock = append(indexBlock, indexEntry...)

			currBlock++
		}

		i += copy(block[i:], item)
	}

	dataBlocks = append(dataBlocks, block...)
	header := createHeader(len(dataBlocks), len(indexBlock))
	data := append(header, append(dataBlocks, indexBlock...)...)

	var fileID string
	var filename string

	if below != "" {
		filenameArr := strings.Split(below, ".")
		if len(filenameArr) != 2 {
			return errors.New("Improper format of sst file")
		}
		directoryArr := strings.Split(filenameArr[0], "/")

		fileID = directoryArr[len(directoryArr)-1]
		filename = filenameArr[0] + "_new.sst"
	} else {
		fileID = level.getUniqueID()
		filename = level.directory + fileID + ".sst"
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return errors.New("Num bytes written during merge does not match expected")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	if below != "" {
		err := os.Rename(filename, below)
		if err != nil {
			return err
		}
		level.manifestLock.Lock()
		level.manifest[fileID] = &keyRange{startKey: startKey, endKey: endKey}
		level.manifestLock.Unlock()
	} else {
		level.NewSSTFile(fileID, startKey, endKey)
	}

	level.size += len(data)

	return nil
}

// Range gets all files at a specific level whose key range fall within the given range query.
// It then concurrently reads all files and returns the result to the given channel
func (level *Level) Range(startKey, endKey string, replyChan chan *LSMRange) {
	filenames := level.RangeSSTFiles(startKey, endKey)

	replies := make(chan *LSMRange)
	var wg sync.WaitGroup
	var errs []error

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go func(filename string) {
			fileRangeQuery(filename, startKey, endKey, replies)
		}(filename)
	}

	result := []*LSMFind{}
	go func() {
		for reply := range replies {
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
		replyChan <- &LSMRange{
			lsmFinds: result,
			err:      errs[0],
		}
		return
	}
	replyChan <- &LSMRange{
		lsmFinds: result,
		err:      nil,
	}
}

func (level *Level) run() {
	updateManifest := time.NewTicker(1 * time.Second)
	exceedCapacity := time.NewTicker(500 * time.Millisecond)

	for {
		select {
		case compact := <-level.compactReqChan:
			merging := []*merge{}

			// Place manifest into merging array
			level.manifestLock.RLock()
			level.mergeLock.RLock()
			for filename, keyRange := range level.manifest {
				if _, ok := level.merging[filename]; !ok {
					merging = append(merging, &merge{
						aboveFiles: []string{},
						belowFile:  level.directory + filename + ".sst",
						keyRange:   keyRange,
					})
				}
			}
			level.manifestLock.RUnlock()
			level.mergeLock.RUnlock()

			for _, m1 := range compact {
				sk1 := m1.keyRange.startKey
				ek1 := m1.keyRange.endKey
				merged := false

				for i, m2 := range merging {
					sk2 := m2.keyRange.startKey
					ek2 := m2.keyRange.endKey

					if (sk1 >= sk2 && sk1 <= ek2) || (ek1 >= sk2 && ek1 <= ek2) {
						merging[i].aboveFiles = append(merging[i].aboveFiles, m1.aboveFiles...)

						if sk1 < sk2 {
							merging[i].keyRange.startKey = sk1
						}
						if ek1 > ek2 {
							merging[i].keyRange.endKey = ek1
						}

						merged = true
						break
					}
				}
				if !merged {
					merging = append(merging, m1)
				}
			}

			var wg sync.WaitGroup

			for _, mergeStruct := range merging {
				if len(mergeStruct.aboveFiles) > 0 {
					wg.Add(1)
					go func(filename string, files []string) {
						defer wg.Done()
						level.Merge(filename, files)
					}(mergeStruct.belowFile, mergeStruct.aboveFiles)
				}
			}

			wg.Wait()
		case reply := <-level.compactReplyChan:
			if reply.err != nil {
				fmt.Println(reply.err)
			} else {
				err := level.DeleteSSTFiles(reply.mergedFiles)
				if err != nil {
					fmt.Println(err)
				}
			}
		case <-updateManifest.C:
			err := level.UpdateManifest()
			if err != nil {
				fmt.Println(err)
			}
		case <-exceedCapacity.C:
			if level.size > level.capacity {
				level.size = 0
				compact := level.mergeManifest()
				level.below.compactReqChan <- compact
			}
		}
	}
}
