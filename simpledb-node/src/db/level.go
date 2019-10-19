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

func (level *Level) Merge(below string, above []string) {
	var values [][]byte

	mergedAbove, err := level.mergeAbove(above)
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
	if level.size > level.capacity {
		level.size = 0
		compact := level.mergeManifest()
		level.below.compactReqChan <- compact
	}
	return nil
}

func (level *Level) mergeAbove(above []string) ([][]byte, error) {
	if len(above) > 1 {
		mid := len(above) / 2

		left, err := level.mergeAbove(above[:mid])
		if err != nil {
			return nil, err
		}
		right, err := level.mergeAbove(above[mid:])
		if err != nil {
			return nil, err
		}

		result := mergeHelper(left, right)

		return result, nil
	}

	return mmap(above[0])
}

func mergeHelper(left, right [][]byte) [][]byte {
	od := NewOrderedDict()
	i, j := 0, 0

	for i < len(left) && j < len(right) {
		leftEntry := NewODValue(left[i])
		rightEntry := NewODValue(right[j])

		if val, ok := od.Get(leftEntry.Key()); ok {
			if val.(odValue).Offset() < leftEntry.Offset() {
				od.Set(leftEntry.Key(), leftEntry)
			}
			i++
			continue
		}

		if val, ok := od.Get(rightEntry.Key()); ok {
			if val.(odValue).Offset() < rightEntry.Offset() {
				od.Set(rightEntry.Key(), rightEntry)
			}
			j++
			continue
		}

		if leftEntry.Key() < rightEntry.Key() {
			od.Set(leftEntry.Key(), leftEntry)
			i++
		} else if leftEntry.Key() == rightEntry.Key() {
			if leftEntry.Offset() > rightEntry.Offset() {
				od.Set(leftEntry.Key(), leftEntry)
			} else {
				od.Set(rightEntry.Key(), rightEntry)
			}
			i++
			j++
		} else {
			od.Set(rightEntry.Key(), rightEntry)
			j++
		}
	}

	for i < len(left) {
		leftEntry := NewODValue(left[i])
		od.Set(leftEntry.Key(), leftEntry)
		i++
	}

	for j < len(right) {
		rightEntry := NewODValue(right[j])
		od.Set(rightEntry.Key(), rightEntry)
		j++
	}

	result := [][]byte{}

	for val := range od.Iterate() {
		result = append(result, val.(odValue).Entry())
	}

	return result
}

func mmap(filename string) ([][]byte, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	dataSize, _, err := readHeader(f)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, dataSize)

	numBytes, err := f.ReadAt(buffer, 16)
	if err != nil {
		return nil, err
	}

	if numBytes != len(buffer) {
		return nil, errors.New("Num bytes read from file does not match expected data block size")
	}

	keys := [][]byte{}

	for i := 0; i < len(buffer); i += blockSize {
		block := buffer[i : i+blockSize]
		j := 0
		for j < len(block) && block[j] != byte(0) {
			keySize := uint8(block[j])
			j++
			entry := make([]byte, 13+int(keySize))
			copy(entry[0:], []byte{keySize})
			copy(entry[1:], block[j:j+int(keySize)+12])
			j += int(keySize) + 12
			keys = append(keys, entry)
		}
	}

	return keys, nil
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

func (level *Level) run() {
	updateManifest := time.NewTicker(1 * time.Second)
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
				fmt.Println("Done compacting")
			}
		case <-updateManifest.C:
			err := level.UpdateManifest()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}
