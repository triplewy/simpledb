package db

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
)

type Level struct {
	level    int
	capacity int
	filename string
}

type SSTable struct {
	level0 *Level
	level1 *Level
}

func NewLevel(level int) (*Level, error) {
	f, err := os.OpenFile("level"+strconv.Itoa(level)+".sst", os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	return &Level{
		level:    level,
		capacity: blockSize,
		filename: "level" + strconv.Itoa(level) + ".sst",
	}, nil
}

func NewSSTable() (*SSTable, error) {
	level0, err := NewLevel(0)
	if err != nil {
		return nil, err
	}

	level1, err := NewLevel(1)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		level0: level0,
		level1: level1,
	}, nil
}

func (table *SSTable) Append(data []byte) error {
	f, err := os.OpenFile(table.level0.filename, os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	block, err := NewBlock(data)
	if err != nil {
		return err
	}

	_, err = f.Write(block.data[:])
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (table *SSTable) Find(key string) (offset uint64, size uint32, err error) {
	f, err := os.OpenFile(table.level0.filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return 0, 0, err
	}

	buffer := make([]byte, blockSize)

	for {
		numBytes, err := f.Read(buffer)

		if err != nil {
			if err != io.EOF {
				return 0, 0, err
			}
			break
		}

		i := 0
		for i < numBytes && buffer[i] != byte(0) {
			keySize := uint8(buffer[i])
			i++
			result := buffer[i : i+int(keySize)]
			i += int(keySize)
			valueOffset := binary.LittleEndian.Uint64(buffer[i : i+8])
			i += 8
			valueSize := binary.LittleEndian.Uint32(buffer[i : i+4])
			i += 4

			if key == string(result) {
				return valueOffset, valueSize, nil
			}
		}
	}
	return 0, 0, nil
}
