package db

import (
	"math"

	"github.com/spaolacci/murmur3"
)

type Bloom struct {
	k    uint32
	size uint32
	bits []byte
}

func NewBloom(n int) *Bloom {
	k := uint32(10) // Number of hash functions
	p := 0.001      // False positive probability 0.001 = 1/1000 False Positive = 99.9% Correct
	size := uint32(math.Ceil((float64(n) * math.Log(p)) / math.Log(1/math.Pow(2, math.Log(2)))))

	return &Bloom{
		k:    k,
		size: size,
		bits: make([]byte, size),
	}
}

func (bloom *Bloom) Insert(key string) {
	for i := uint32(0); i < bloom.k; i++ {
		hasher := murmur3.New32WithSeed(i)
		hasher.Write([]byte(key))
		index := hasher.Sum32() % bloom.size
		bloom.bits[index] = byte(1)
	}
}

func (bloom *Bloom) Check(key string) bool {
	for i := uint32(0); i < bloom.k; i++ {
		hasher := murmur3.New32WithSeed(i)
		hasher.Write([]byte(key))
		index := hasher.Sum32() % bloom.size
		if bloom.bits[index] == byte(0) {
			return false
		}
	}
	return true
}
