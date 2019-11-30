package db

import (
	"encoding/binary"
	"math"
)

// lsmDataEntry is struct that represents an entry into an lsm Data Block
type lsmDataEntry struct {
	ts        uint64
	keySize   uint8
	key       string
	valueType uint8
	valueSize uint16
	value     []byte
}

func createDataEntry(ts uint64, key string, value interface{}) (*lsmDataEntry, error) {
	if len(key) > KeySize {
		return nil, newErrExceedMaxKeySize(key)
	}
	keySize := uint8(len(key))
	entry := &lsmDataEntry{
		ts:        ts,
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
			return nil, newErrExceedMaxValueSize()
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
		return nil, newErrNoTypeFound()
	}
}

func encodeDataEntry(entry *lsmDataEntry) (data []byte) {
	tsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBytes, entry.ts)
	data = append(data, tsBytes...)
	data = append(data, entry.keySize)
	data = append(data, []byte(entry.key)...)
	data = append(data, entry.valueType)
	valueSizeBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(valueSizeBytes, entry.valueSize)
	data = append(data, valueSizeBytes...)
	data = append(data, entry.value...)
	return data
}

func decodeDataEntry(data []byte) *lsmDataEntry {
	return nil
}

func parseDataEntry(entry *lsmDataEntry) (*kv, error) {
	kv := &kv{
		ts:  entry.ts,
		key: entry.key,
	}
	value := entry.value
	switch entry.valueType {
	case Bool:
		if len(value) != 1 {
			return nil, newErrIncorrectValueSize(Bool, len(value))
		}
		if value[0] != byte(0) && value[0] != byte(1) {
			return nil, newErrIncompatibleValue(Bool)
		}
		if value[0] == byte(0) {
			kv.value = false
		} else {
			kv.value = true
		}
	case Int:
		if len(value) != 8 {
			return nil, newErrIncorrectValueSize(Int, len(value))
		}
		i := int64(binary.LittleEndian.Uint64(value))
		kv.value = i
	case Float:
		if len(value) != 8 {
			return nil, newErrIncorrectValueSize(Float, len(value))
		}
		f := float64(binary.LittleEndian.Uint64(value))
		kv.value = f
	case String:
		kv.value = string(value)
	case Tombstone:
		return nil, newErrKeyNotFound()
	default:
		return nil, newErrNoTypeFound()
	}
	return kv, nil
}

func sizeDataEntry(entry *lsmDataEntry) int {
	return EntrySizeConstant + len(entry.key) + len(entry.value)
}

func writeDataEntries(entries []*lsmDataEntry) (dataBlocks, indexBlock []byte, bloom *bloom, kr *keyRange, err error) {
	kr = &keyRange{
		startKey: entries[0].key,
		endKey:   entries[len(entries)-1].key,
	}
	bloom = newBloom(len(entries))

	block := make([]byte, BlockSize)
	currBlock := uint32(0)
	i := 0
	for index, entry := range entries {
		// Create new block if current entry overflows block
		if i+sizeDataEntry(entry) >= BlockSize {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&lsmIndexEntry{
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
			indexEntry := encodeIndexEntry(&lsmIndexEntry{
				keySize: entry.keySize,
				key:     entry.key,
				block:   currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
		}
	}
	return dataBlocks, indexBlock, bloom, kr, nil
}

// lsmIndexEntry is struct that represents an entry into an lsm Index Block
type lsmIndexEntry struct {
	keySize uint8
	key     string
	block   uint32
}

func encodeIndexEntry(entry *lsmIndexEntry) (data []byte) {
	data = append(data, entry.keySize)
	data = append(data, []byte(entry.key)...)
	blockBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockBytes, entry.block)
	data = append(data, blockBytes...)
	return data
}
