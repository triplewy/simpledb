package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type keyRange struct {
	startKey string
	endKey   string
}

type merge struct {
	files    []string
	keyRange *keyRange
}

type compactReply struct {
	mergedFiles []string
	err         error
}

type Level struct {
	level          int
	blockCapacity  int
	indexBlockSize int
	directory      string

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

func NewLevel(level, blockCapacity int) *Level {
	l := &Level{
		level:          level,
		blockCapacity:  blockCapacity,
		indexBlockSize: blockCapacity * (3 + keySize),
		directory:      "data/L" + strconv.Itoa(level) + "/",
		merging:        make(map[string]struct{}),

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
	mergedAbove, err := level.mergeAbove(above)
	if err != nil {
		level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
		return
	}

	startItem := mergedAbove[0]
	startKeySize := uint8(startItem[0])
	startKey := string(startItem[1 : 1+startKeySize])

	endItem := mergedAbove[len(mergedAbove)-1]
	endKeySize := uint8(endItem[0])
	endKey := string(endItem[1 : 1+endKeySize])

	if below == "empty" {
		indexBlock := make([]byte, level.indexBlockSize)
		dataBlocks := []byte{}
		block := make([]byte, blockSize)
		currBlock := 0

		i, j := 0, 0
		for _, item := range mergedAbove {
			if i+len(item) > blockSize {
				dataBlocks = append(dataBlocks, block...)
				block = make([]byte, blockSize)
				i = 0
			}
			if i == 0 {
				keySize := uint8(item[0])
				indexEntry := make([]byte, keySize+3)
				key := item[1 : 1+keySize]
				currBlockBytes := make([]byte, 2)
				binary.LittleEndian.PutUint16(currBlockBytes, uint16(currBlock))

				copy(indexEntry[0:], []byte{keySize})
				copy(indexEntry[1:], key)
				copy(indexEntry[1+keySize:], currBlockBytes)

				currBlock++

				j += copy(indexBlock[j:], indexEntry)
			}

			i += copy(block[i:], item)
		}

		dataBlocks = append(dataBlocks, block...)

		data := append(dataBlocks, indexBlock...)

		filename := level.getUniqueID()

		f, err := os.OpenFile(level.directory+filename+".sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
		defer f.Close()

		if err != nil {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
			return
		}

		numBytes, err := f.Write(data)
		if err != nil {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
			return
		}
		if numBytes != len(data) {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: errors.New("Num bytes written during merge does not match expected")}
			return
		}

		err = f.Sync()
		if err != nil {
			level.compactReplyChan <- &compactReply{mergedFiles: nil, err: err}
			return
		}

		level.NewSSTFile(filename, startKey, endKey)
	} else {
		fmt.Println("Merge below:", below)
	}

	level.above.compactReplyChan <- &compactReply{mergedFiles: above, err: nil}
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

	return mmap(above[0], level.above.indexBlockSize)
}

func mergeHelper(left, right [][]byte) [][]byte {
	i, j := 0, 0

	result := [][]byte{}

	for i < len(left) && j < len(right) {
		leftKeySize := uint8(left[i][0])
		leftKey := left[i][1 : 1+leftKeySize]

		rightKeySize := uint8(right[j][0])
		rightKey := right[j][1 : 1+rightKeySize]

		if string(leftKey) < string(rightKey) {
			result = append(result, left[i])
			i++
		} else {
			result = append(result, right[j])
			j++
		}
	}

	for i < len(left) {
		result = append(result, left[i])
		i++
	}

	for j < len(right) {
		result = append(result, right[j])
		j++
	}

	return result
}

func mmap(filename string, indexBlockSize int) ([][]byte, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	totalSize := info.Size()
	buffer := make([]byte, totalSize-int64(indexBlockSize))
	numBytes, err := f.Read(buffer)
	if err != nil {
		return nil, err
	}

	if numBytes != len(buffer) {
		return nil, errors.New("Num bytes read from file does not match expected data block size")
	}

	keys := [][]byte{}
	i := 0

	for i < len(buffer) {
		keySize := uint8(buffer[0])
		i++
		entry := make([]byte, 13+keySize)
		copy(entry[0:], []byte{keySize})
		copy(entry[1:], buffer[i:i+int(keySize)+12])
		i += int(keySize) + 12
		keys = append(keys, entry)
	}

	return keys, nil
}

func (level *Level) getUniqueID() string {
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
			merging := make(map[string]*merge)

			level.manifestLock.RLock()
			for filename, keyRange := range level.manifest {
				merging[filename] = &merge{files: []string{}, keyRange: keyRange}
			}
			level.manifestLock.RUnlock()

			noMergeCandidates := []string{}

			for _, m1 := range compact {
				f1 := m1.files[0]
				kr1 := m1.keyRange
				sk1 := kr1.startKey
				ek1 := kr1.endKey
				foundMergeCandidate := false

				for f2, m2 := range merging {
					kr2 := m2.keyRange
					sk2 := kr2.startKey
					ek2 := kr2.endKey

					if (sk1 <= ek2 && sk1 >= sk2) || (ek1 <= ek2 && ek1 >= sk2) {
						foundMergeCandidate = true
						merging[f2].files = append(merging[f2].files, level.above.directory+f1+".sst")

						if sk1 < sk2 {
							merging[f2].keyRange.startKey = sk1
						}

						if ek1 > ek2 {
							merging[f2].keyRange.endKey = ek1
						}
						break
					}
				}
				if !foundMergeCandidate {
					noMergeCandidates = append(noMergeCandidates, level.above.directory+f1+".sst")
				}
			}

			if len(noMergeCandidates) > 0 {
				merging["empty"] = &merge{files: noMergeCandidates, keyRange: nil}
			}

			var wg sync.WaitGroup

			for filename, mergeStruct := range merging {
				if len(mergeStruct.files) > 0 {
					wg.Add(1)
					go func(filename string, files []string) {
						defer wg.Done()
						level.Merge(filename, files)
					}(filename, mergeStruct.files)
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
