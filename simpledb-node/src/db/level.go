package db

import (
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
	level     int
	capacity  int
	directory string
	manifest  map[string]*keyRange

	merging           map[string]*merge
	compactReqChan    chan struct{}
	compactFinishChan chan struct{}
	compactReplyChan  chan string

	above *Level
	below *Level
}

func NewLevel(level int) *Level {
	l := &Level{
		level:             level,
		capacity:          blockSize,
		directory:         "data/L" + strconv.Itoa(level) + "/",
		manifest:          make(map[string]*keyRange),
		merging:           make(map[string]*merge),
		compactReqChan:    make(chan struct{}),
		compactFinishChan: make(chan struct{}),
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

func (level *Level) MergeAbove(above []string) ([][]byte, error) {
	if len(above) > 1 {
		mid := len(above) / 2

		left, err := level.MergeAbove(above[:mid])
		if err != nil {
			return nil, err
		}
		right, err := level.MergeAbove(above[mid:])
		if err != nil {
			return nil, err
		}
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

		return result, nil
	}

	return mmap(above[0])
}

func (level *Level) getUniqueId() string {
	id := uuid.New().String()[:8]
	if _, ok := level.manifest[id]; !ok {
		return id
	}
	return level.getUniqueId()
}

func mmap(filename string) ([][]byte, error) {
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
	buffer := make([]byte, totalSize-indexBlockSize)
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
						level.merging[f2].files = append(level.merging[f2].files, f1)

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
					noMergeCandidates = append(noMergeCandidates, f1)
				}
			}

			level.merging["empty"] = &merge{files: noMergeCandidates, keyRange: nil}
			fmt.Println(noMergeCandidates)
			fmt.Println(level.merging)

		case <-level.compactReplyChan:
			return
		}
	}
}
