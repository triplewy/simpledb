package db

import (
	"encoding/binary"
	"math"
)

// LSMDataEntry is struct that represents an entry into an LSM Data Block
type LSMDataEntry struct {
	seqID     uint64
	keySize   uint8
	key       string
	valueType uint8
	valueSize uint16
	value     []byte
}

func createDataEntry(seqID uint64, key string, value interface{}) (*LSMDataEntry, error) {
	if len(key) > KeySize {
		return nil, ErrExceedMaxKeySize(key)
	}
	keySize := uint8(len(key))
	entry := &LSMDataEntry{
		seqID:     seqID,
		keySize:   keySize,
		key:       key,
		valueType: 0,
		valueSize: 0,
		value:     nil,
	}
	switch v := value.(type) {
	case bool:
		entry.valueType = Bool
		entry.valueSize = 1
		if v {
			entry.value = []byte{1}
		} else {
			entry.value = []byte{0}
		}
		return entry, nil
	case int64:
		entry.valueType = Int
		entry.valueSize = 8
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, uint64(v))
		entry.value = valueBytes
		return entry, nil
	case float64:
		entry.valueType = Float
		entry.valueSize = 8
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, math.Float64bits(v))
		entry.value = valueBytes
		return entry, nil
	case string:
		if len(v) > ValueSize {
			return nil, ErrExceedMaxValueSize()
		}
		entry.valueType = String
		entry.valueSize = uint16(len(v))
		entry.value = []byte(v)
		return entry, nil
	case nil:
		entry.valueType = Tombstone
		entry.valueSize = 0
		entry.value = []byte{}
		return entry, nil
	default:
		return nil, ErrNoTypeFound()
	}
}

func encodeDataEntry(entry *LSMDataEntry) (data []byte) {
	seqIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqIDBytes, entry.seqID)
	data = append(data, seqIDBytes...)
	data = append(data, entry.keySize)
	data = append(data, []byte(entry.key)...)
	data = append(data, entry.valueType)
	valueSizeBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(valueSizeBytes, entry.valueSize)
	data = append(data, valueSizeBytes...)
	data = append(data, entry.value...)
	return data
}

func decodeDataEntry(data []byte) *LSMDataEntry {
	return nil
}

func parseDataEntry(entry *LSMDataEntry) (*KV, error) {
	kv := &KV{
		commitTs: entry.seqID,
		key:      entry.key,
	}
	value := entry.value
	switch entry.valueType {
	case Bool:
		if len(value) != 1 {
			return nil, ErrIncorrectValueSize(Bool, len(value))
		}
		if value[0] != byte(0) && value[0] != byte(1) {
			return nil, ErrIncompatibleValue(Bool)
		}
		if value[0] == byte(0) {
			kv.value = false
		} else {
			kv.value = true
		}
	case Int:
		if len(value) != 8 {
			return nil, ErrIncorrectValueSize(Int, len(value))
		}
		i := int64(binary.LittleEndian.Uint64(value))
		kv.value = i
	case Float:
		if len(value) != 8 {
			return nil, ErrIncorrectValueSize(Float, len(value))
		}
		f := float64(binary.LittleEndian.Uint64(value))
		kv.value = f
	case String:
		kv.value = string(value)
	case Tombstone:
		return nil, ErrKeyNotFound()
	default:
		return nil, ErrNoTypeFound()
	}
	return kv, nil
}

func sizeDataEntry(entry *LSMDataEntry) int {
	return EntrySizeConstant + len(entry.key) + len(entry.value)
}

func writeDataEntries(entries []*LSMDataEntry) (dataBlocks, indexBlock []byte, bloom *Bloom, keyRange *KeyRange, err error) {
	keyRange = &KeyRange{
		startKey: entries[0].key,
		endKey:   entries[len(entries)-1].key,
	}
	bloom = NewBloom(len(entries))

	block := make([]byte, BlockSize)
	currBlock := uint32(0)
	i := 0
	for index, entry := range entries {
		// Create new block if current entry overflows block
		if i+sizeDataEntry(entry) >= BlockSize {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&LSMIndexEntry{
				keySize: entries[index-1].keySize,
				key:     entries[index-1].key,
				block:   currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)

			block = make([]byte, BlockSize)
			currBlock++
			i = 0
		}

		entryData := encodeDataEntry(entry)
		i += copy(block[i:], entryData)

		bloom.Insert(entry.key)

		if index == len(entries)-1 {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&LSMIndexEntry{
				keySize: entry.keySize,
				key:     entry.key,
				block:   currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
		}
	}
	return dataBlocks, indexBlock, bloom, keyRange, nil
}

// LSMIndexEntry is struct that represents an entry into an LSM Index Block
type LSMIndexEntry struct {
	keySize uint8
	key     string
	block   uint32
}

func encodeIndexEntry(entry *LSMIndexEntry) (data []byte) {
	data = append(data, entry.keySize)
	data = append(data, []byte(entry.key)...)
	blockBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockBytes, entry.block)
	data = append(data, blockBytes...)
	return data
}
