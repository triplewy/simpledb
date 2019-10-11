package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"

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

type Level struct {
	level          int
	capacity       int
	indexBlockSize int
	directory      string
	manifest       map[string]*keyRange

	merging           map[string]*merge
	compactReqChan    chan struct{}
	compactFinishChan chan error
	compactReplyChan  chan string

	above *Level
	below *Level
}

func NewLevel(level, indexBlockSize int) *Level {
	l := &Level{
		level:             level,
		capacity:          blockSize,
		indexBlockSize:    indexBlockSize,
		directory:         "data/L" + strconv.Itoa(level) + "/",
		manifest:          make(map[string]*keyRange),
		merging:           make(map[string]*merge),
		compactReqChan:    make(chan struct{}),
		compactFinishChan: make(chan error),
		compactReplyChan:  make(chan string),
		above:             nil,
		below:             nil,
	}

	go l.run()

	return l
}

func (level *Level) NewSSTFile(filename, startKey, endKey string) error {
	f, err := os.OpenFile(level.directory+"manifest_new", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	level.manifest[filename] = &keyRange{startKey: startKey, endKey: endKey}

	manifest := []byte{}

	for filename, item := range level.manifest {
		data := make([]byte, filenameLength+keySize*2+2)
		startKeyBytes := make([]byte, keySize)
		endKeyBytes := make([]byte, keySize)

		copy(startKeyBytes[0:], []byte(item.startKey))
		copy(endKeyBytes[0:], []byte(item.endKey))

		copy(data[0:], []byte(filename))
		copy(data[filenameLength:], startKeyBytes)
		copy(data[filenameLength+keySize:], endKeyBytes)
		copy(data[len(data)-2:], []byte("\r\n"))

		manifest = append(manifest, data[:]...)
	}

	numBytes, err := f.Write(manifest)
	if err != nil {
		return err
	}

	if numBytes != len(level.manifest)*(filenameLength+keySize*2+2) {
		return errors.New("Num bytes written to manifest does not match data")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	err = os.Rename(level.directory+"manifest_new", level.directory+"manifest")
	if err != nil {
		return err
	}

	if len(level.manifest) >= compactThreshold {
		level.below.compactReqChan <- struct{}{}
	}
	return nil
}

func (level *Level) FindSSTFile(key string) (filenames []string) {
	for filename, item := range level.manifest {
		if key >= item.startKey && key <= item.endKey {
			filenames = append(filenames, level.directory+filename+".sst")
		}
	}
	return filenames
}

func (level *Level) Merge(below string, above []string) {
	mergedAbove, err := level.mergeAbove(above)
	if err != nil {
		level.compactFinishChan <- err
		return
	}
	startItem := mergedAbove[0]
	startKeySize := uint8(startItem[0])
	startKey := string(startItem[1 : 1+startKeySize])

	endItem := mergedAbove[len(mergedAbove)-1]
	endKeySize := uint8(endItem[0])
	endKey := string(endItem[1 : 1+endKeySize])

	if below == "empty" {
		indexBlock := []byte{}
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

		data := append(dataBlocks, indexBlock...)

		filename := level.getUniqueId()

		f, err := os.OpenFile(level.directory+filename+".sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
		defer f.Close()

		if err != nil {
			level.compactFinishChan <- err
			return
		}

		numBytes, err := f.Write(data)
		if err != nil {
			level.compactFinishChan <- err
			return
		}
		if numBytes != len(dataBlocks) {
			level.compactFinishChan <- err
			return
		}

		err = level.NewSSTFile(filename, startKey, endKey)
		if err != nil {
			level.compactFinishChan <- err
			return
		}
	}

	level.compactFinishChan <- nil
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

func (level *Level) getUniqueId() string {
	id := uuid.New().String()[:8]
	if _, ok := level.manifest[id]; !ok {
		return id
	}
	return level.getUniqueId()
}

func (level *Level) run() {
	for {
		select {
		case <-level.compactReqChan:
			for f, keyRange := range level.manifest {
				level.merging[f] = &merge{files: []string{}, keyRange: keyRange}
			}

			noMergeCandidates := []string{}

			for f1, keyRange1 := range level.above.manifest {
				startKey := keyRange1.startKey
				endKey := keyRange1.endKey
				foundMergeCandidate := false
				for f2, merge := range level.merging {
					kr := merge.keyRange
					if (startKey <= kr.endKey && startKey >= kr.startKey) || (endKey <= kr.endKey && endKey >= kr.startKey) {
						foundMergeCandidate = true
						level.merging[f2].files = append(level.merging[f2].files, level.above.directory+f1+".sst")

						if startKey < kr.startKey {
							level.merging[f2].keyRange.startKey = startKey
						}

						if endKey > kr.endKey {
							level.merging[f2].keyRange.endKey = endKey
						}
						break
					}
				}
				if !foundMergeCandidate {
					noMergeCandidates = append(noMergeCandidates, level.above.directory+f1+".sst")
				}
			}

			level.merging["empty"] = &merge{files: noMergeCandidates, keyRange: nil}

			for filename, mergeStruct := range level.merging {
				go func(filename string, files []string) {
					level.Merge(filename, files)
				}(filename, mergeStruct.files)
			}
		case err := <-level.compactFinishChan:
			if err != nil {
				fmt.Println(err)
			}
		case <-level.compactReplyChan:
			return

		}
	}
}
