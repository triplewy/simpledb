package db

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"sync"
)

type SSTable struct {
	levels []*Level
}

type LSMFind struct {
	offset uint64
	size   uint32
	err    error
}

func NewSSTable() (*SSTable, error) {
	levels := []*Level{}

	for i := 0; i < 7; i++ {
		var blockCapacity int
		if i == 0 {
			blockCapacity = 2 * 1048576
		} else {
			blockCapacity = int(math.Pow10(i)) * 1048576
		}

		level := NewLevel(i, blockCapacity)

		if i > 0 {
			above := levels[i-1]
			above.below = level
			level.above = above
		}
		levels = append(levels, level)
	}

	return &SSTable{levels: levels}, nil
}

func (table *SSTable) Append(blocks, index []byte, startKey, endKey string) error {
	var f *os.File
	var err error

	level := table.levels[0]
	filename := level.getUniqueID()

	f, err = os.OpenFile(level.directory+filename+".sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	header := createHeader(len(blocks), len(index))

	_, err = f.Write(append(header, append(blocks, index...)...))
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	level.NewSSTFile(filename, startKey, endKey)

	return nil
}

func (table *SSTable) Find(key string, levelNum int) (*LSMFind, error) {
	if levelNum > 6 {
		return nil, errors.New("Find exceeds greatest level")
	}

	level := table.levels[levelNum]

	filenames := level.FindSSTFile(key)
	if len(filenames) == 0 {
		return table.Find(key, levelNum+1)
	}

	replyChan := make(chan *LSMFind)
	replies := []*LSMFind{}

	var wg sync.WaitGroup
	var errs []error

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go func(filename string) {
			table.find(filename, key, replyChan)
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
		if err.Error() != "Key not found" {
			return nil, err
		}
	}

	return table.Find(key, levelNum+1)
}

func (table *SSTable) find(filename, key string, replyChan chan *LSMFind) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	dataSize, indexSize, err := readHeader(f)
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, 16+int64(dataSize))
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	if numBytes != int(indexSize) {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    errors.New("Did not read correct amount of bytes for index"),
		}
		return
	}

	blockIndex := findDataBlock(key, index)

	block := make([]byte, blockSize)
	numBytes, err = f.ReadAt(block, 16+int64(blockSize*int(blockIndex)))
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}
	if numBytes != blockSize {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    errors.New("Did not read correct amount of bytes for data block"),
		}
		return
	}

	offset, size, err := findKeyInBlock(key, block)
	replyChan <- &LSMFind{
		offset: offset,
		size:   size,
		err:    err,
	}
}

func findDataBlock(key string, index []byte) uint32 {
	i := 0
	block := uint32(0)
	for i < len(index) {
		size := uint8(index[i])
		i++
		indexKey := string(index[i : i+int(size)])
		i += int(size)

		if key < indexKey {
			return binary.LittleEndian.Uint32(index[i:i+4]) - 1
		} else if key == indexKey {
			return binary.LittleEndian.Uint32(index[i : i+4])
		}
		i += 4
		block++
	}

	return block - 1
}

func findKeyInBlock(key string, block []byte) (offset uint64, size uint32, err error) {
	i := 0

	for i < blockSize {
		size := uint8(block[i])
		i++
		foundKey := string(block[i : i+int(size)])
		i += int(size)
		if key == foundKey {
			valueOffset := binary.LittleEndian.Uint64(block[i : i+8])
			i += 8
			valueSize := binary.LittleEndian.Uint32(block[i : i+4])
			i += 4
			return valueOffset, valueSize, nil
		}
		i += 12
	}

	return 0, 0, errors.New("Key not found")
}

func createHeader(dataSize, indexSize int) []byte {
	dataSizeBytes := make([]byte, 8)
	indexSizeBytes := make([]byte, 8)

	binary.LittleEndian.PutUint64(dataSizeBytes, uint64(dataSize))
	binary.LittleEndian.PutUint64(indexSizeBytes, uint64(indexSize))

	header := append(dataSizeBytes, indexSizeBytes...)

	return header
}

func readHeader(f *os.File) (dataSize, indexSize uint64, err error) {
	header := make([]byte, 16)

	numBytes, err := f.Read(header)
	if err != nil {
		return 0, 0, err
	}
	if numBytes != len(header) {
		return 0, 0, errors.New("Num bytes read does nat match expect header size")
	}

	dataSize = binary.LittleEndian.Uint64(header[:8])
	indexSize = binary.LittleEndian.Uint64(header[8:])

	return dataSize, indexSize, nil
}
