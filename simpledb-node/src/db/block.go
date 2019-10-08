package db

import (
	"errors"
	"strconv"
)

type block struct {
	data [blockSize]byte
}

func NewBlock(data []byte) (*block, error) {
	block := &block{data: [...]byte{blockSize - 1: 0}}
	if len(data) > blockSize {
		return nil, errors.New("data larger than " + strconv.Itoa(blockSize) + " bytes")
	}
	copy(block.data[0:], data)
	return block, nil
}
