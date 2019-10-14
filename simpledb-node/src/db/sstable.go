package db

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
)

const l0Size = 2 * 1024

// const l1Size = 10 * 1024 / blockSize

// const l0IndexSize = l0Size * (3 + keySize)
// const l1IndexSize = l1Size * (3 + keySize)

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
			blockCapacity = 2 * 1024 / blockSize
		} else {
			blockCapacity = int(math.Pow10(i)) * 1024 / blockSize
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

	filename := level.getUniqueId()

	f, err = os.OpenFile(level.directory+filename+".sst", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	filler := make([]byte, level.indexBlockSize-len(index))
	index = append(index, filler...)

	if len(index) != level.indexBlockSize {
		return errors.New("LSM index block does not match 16 KB")
	}

	_, err = f.Write(append(blocks, index...))
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

func (table *SSTable) Find(key string, levelNum int) ([]*LSMFind, error) {
	if levelNum > 6 {
		return nil, errors.New("Find exceeds greatest level")
	}

	level := table.levels[levelNum]

	filenames := level.FindSSTFile(key)
	if len(filenames) == 0 {
		return table.Find(key, levelNum+1)
	}

	replyChan := make(chan *LSMFind, len(filenames))

	for _, filename := range filenames {
		go func(filename string) {
			table.find(filename, key, levelNum, replyChan)
		}(filename)
	}

	replies := []*LSMFind{}

	for i := 0; i < len(filenames); i++ {
		reply := <-replyChan
		if reply.err != nil {
			return nil, reply.err
		}
		replies = append(replies, reply)
	}

	return replies, nil
}

func (table *SSTable) find(filename, key string, levelNum int, replyChan chan *LSMFind) {
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

	level := table.levels[levelNum]

	info, err := f.Stat()
	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	totalSize := info.Size()
	indexSize := level.indexBlockSize
	index := make([]byte, indexSize)
	numBytes, err := f.ReadAt(index, totalSize-int64(indexSize))

	if err != nil {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    err,
		}
		return
	}

	if numBytes != indexSize {
		replyChan <- &LSMFind{
			offset: 0,
			size:   0,
			err:    errors.New("Did not read correct amount of bytes for index"),
		}
		return
	}

	blockIndex := findDataBlock(key, index)

	block := make([]byte, blockSize)
	numBytes, err = f.ReadAt(block, int64(blockSize*blockIndex))
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

func findDataBlock(key string, index []byte) uint16 {
	i := 0
	block := uint16(0)
	for i < len(index) {
		size := uint8(index[i])
		i++
		indexKey := string(index[i : i+int(size)])
		i += int(size)

		if key < indexKey {
			return binary.LittleEndian.Uint16(index[i:i+2]) - 1
		} else if key == indexKey {
			return binary.LittleEndian.Uint16(index[i : i+2])
		}

		i += 2
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
