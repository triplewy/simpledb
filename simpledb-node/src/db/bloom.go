package db

import (
	"math"

	"github.com/spaolacci/murmur3"
)

// bloom is struct for bloom Filter
type bloom struct {
	k    uint32
	size uint64
	bits []byte
}

// newBloom creates a bloom filter depending on n, the number of elements that will be inserted into the bloom filter
func newBloom(n int) *bloom {
	k := uint32(10) // Number of hash functions
	p := 0.001      // False positive probability 0.001 = 1/1000 False Positive = 99.9% Correct
	size := uint64(math.Ceil((float64(n) * math.Log(p)) / math.Log(1/math.Pow(2, math.Log(2)))))

	return &bloom{
		k:    k,
		size: size,
		bits: make([]byte, size),
	}
}

// recoverBloom creates a new in-memory bloom filter from a bits array
func recoverBloom(bits []byte) *bloom {
	k := uint32(10)

	return &bloom{
		k:    k,
		size: uint64(len(bits)),
		bits: bits,
	}
}

// Insert inserts a key into the bloom filter
func (bloom *bloom) Insert(key string) {
	for i := uint32(0); i < bloom.k; i++ {
		hasher := murmur3.New64WithSeed(i)
		hasher.Write([]byte(key))
		index := hasher.Sum64() % bloom.size
		bloom.bits[index] = byte(1)
	}
}

// Check checks if a key exists in the bloom filter
func (bloom *bloom) Check(key string) bool {
	for i := uint32(0); i < bloom.k; i++ {
		hasher := murmur3.New64WithSeed(i)
		hasher.Write([]byte(key))
		index := hasher.Sum64() % bloom.size
		if bloom.bits[index] == byte(0) {
			return false
		}
	}
	return true
}
