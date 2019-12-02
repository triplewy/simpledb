package db

import (
	"encoding/binary"
	"math"
)

// Entry represents a row in the db where a key is mapped to multiple fields
type Entry struct {
	ts     uint64
	key    string
	fields map[string]*Value
}

// Value combines a slice of bytes with a data type in order to parse data
type Value struct {
	dataType uint8
	data     []byte
}

func parseValue(value *Value) (interface{}, error) {
	data := value.data
	switch value.dataType {
	case Bool:
		if len(data) != 1 {
			return nil, newErrParseValue(value)
		}
		if data[0] == byte(0) {
			return false, nil
		}
		if data[0] == byte(1) {
			return true, nil
		}
		return nil, newErrParseValue(value)
	case Int:
		if len(data) != 8 {
			return nil, newErrParseValue(value)
		}
		return int64(binary.LittleEndian.Uint64(data)), nil
	case Float:
		if len(data) != 8 {
			return nil, newErrParseValue(value)
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data)), nil
	case String:
		return string(data), nil
	case Bytes:
		return data, nil
	default:
		return nil, newErrParseValue(value)
	}
}

// indexEntry is struct that represents an entry into an lsm Index Block
type indexEntry struct {
	key   string
	block uint32
}

func createEntry(ts uint64, key string, fields map[string]interface{}) (*Entry, error) {
	if len(key) > KeySize {
		return nil, newErrExceedMaxKeySize(key)
	}
	if len(fields) > MaxFields {
		return nil, newErrExceedMaxFields()
	}
	entry := &Entry{
		ts:     ts,
		key:    key,
		fields: make(map[string]*Value),
	}
	for name, data := range fields {
		switch v := data.(type) {
		case bool:
			value := []byte{0}
			if v {
				value = []byte{1}
			}
			entry.fields[name] = &Value{dataType: Bool, data: value}
		case int64:
			value := make([]byte, 8)
			binary.LittleEndian.PutUint64(value, uint64(v))
			entry.fields[name] = &Value{dataType: Int, data: value}
		case float64:
			value := make([]byte, 8)
			binary.LittleEndian.PutUint64(value, math.Float64bits(v))
			entry.fields[name] = &Value{dataType: Float, data: value}
		case string:
			entry.fields[name] = &Value{dataType: String, data: []byte(v)}
		case []byte:
			entry.fields[name] = &Value{dataType: Bytes, data: v}
		case nil:
			entry.fields[name] = &Value{dataType: Tombstone, data: []byte{}}
		default:
			return nil, newErrNoTypeFound()
		}
	}
	totalSize := 0
	for _, value := range entry.fields {
		totalSize += len(value.data)
		if totalSize > EntrySize {
			return nil, newErrExceedMaxEntrySize()
		}
	}
	return entry, nil
}

func encodeEntry(entry *Entry) (data []byte) {
	tsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBytes, entry.ts)
	keySizeBytes := uint8(len(entry.key))
	keyBytes := []byte(entry.key)
	fieldsBytes := []byte{}

	for name, value := range entry.fields {
		nameSizeBytes := uint8(len(name))
		nameBytes := []byte(name)
		dataTypeBytes := value.dataType
		dataSizeBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(dataSizeBytes, uint16(len(value.data)))
		dataBytes := value.data

		fieldBytes := []byte{}
		fieldBytes = append(fieldBytes, nameSizeBytes)
		fieldBytes = append(fieldBytes, nameBytes...)
		fieldBytes = append(fieldBytes, dataTypeBytes)
		fieldBytes = append(fieldBytes, dataSizeBytes...)
		fieldBytes = append(fieldBytes, dataBytes...)

		fieldsBytes = append(fieldsBytes, fieldBytes...)
	}

	data = append(data, tsBytes...)
	data = append(data, keySizeBytes)
	data = append(data, keyBytes...)
	data = append(data, fieldsBytes...)

	totalSize := uint32(len(data))
	totalSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalSizeBytes, totalSize)

	data = append(totalSizeBytes, data...)
	return data
}

func decodeEntry(data []byte) (*Entry, error) {
	const (
		tsBytes uint8 = iota
		keyBytes
		fieldBytes
	)
	fields := make(map[string]*Value)
	entry := &Entry{
		ts:     0,
		key:    "",
		fields: nil,
	}
	step := tsBytes
	i := 0
	for i < len(data) {
		switch step {
		case tsBytes:
			if i+8 > len(data) {
				return nil, newErrDecodeEntry()
			}
			entry.ts = binary.LittleEndian.Uint64(data[i : i+8])
			i += 8
			step = keyBytes
		case keyBytes:
			keySize := uint8(data[i])
			i++
			if i+int(keySize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			entry.key = string(data[i : i+int(keySize)])
			i += int(keySize)
			step = fieldBytes
		case fieldBytes:
			fieldNameSize := uint8(data[i])
			i++
			if i+int(fieldNameSize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			fieldName := string(data[i : i+int(fieldNameSize)])
			i += int(fieldNameSize)
			fieldType := uint8(data[i])
			i++
			if i+2 > len(data) {
				return nil, newErrDecodeEntry()
			}
			fieldDataSize := binary.LittleEndian.Uint16(data[i : i+2])
			i += 2
			if i+int(fieldDataSize) > len(data) {
				return nil, newErrDecodeEntry()
			}
			fieldData := data[i : i+int(fieldDataSize)]
			fields[fieldName] = &Value{dataType: fieldType, data: fieldData}
			i += int(fieldDataSize)
		default:
			return nil, newErrDecodeEntry()
		}
	}
	if len(fields) > 0 {
		entry.fields = fields
	}
	return entry, nil
}

func decodeEntries(data []byte) (entries []*Entry, err error) {
	for i := 0; i < len(data); i += BlockSize {
		block := data[i : i+BlockSize]
		j := 0
		for j < len(block) {
			if j+4 > len(block) {
				break
			}
			entrySize := binary.LittleEndian.Uint32(block[j : j+4])
			j += 4
			if j+int(entrySize) > len(block) {
				return nil, newErrBadFormattedSST()
			}
			if entrySize == 0 {
				break
			}
			entry, err := decodeEntry(block[j : j+int(entrySize)])
			if err != nil {
				return nil, err
			}
			j += int(entrySize)
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func writeEntries(entries []*Entry) (dataBlocks, indexBlock []byte, bloom *bloom, kr *keyRange, err error) {
	kr = &keyRange{
		startKey: entries[0].key,
		endKey:   entries[len(entries)-1].key,
	}
	bloom = newBloom(len(entries))
	block := make([]byte, BlockSize)
	currBlock := uint32(0)
	i := 0
	for index, entry := range entries {
		entryBytes := encodeEntry(entry)
		// Create new block if current entry overflows block
		if i+len(entryBytes) > BlockSize {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&indexEntry{
				key:   entries[index-1].key,
				block: currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
			block = make([]byte, BlockSize)
			currBlock++
			i = 0
		}
		i += copy(block[i:], entryBytes)
		bloom.Insert(entry.key)
		// If last entry, append data block and index entry
		if index == len(entries)-1 {
			dataBlocks = append(dataBlocks, block...)
			indexEntry := encodeIndexEntry(&indexEntry{
				key:   entry.key,
				block: currBlock,
			})
			indexBlock = append(indexBlock, indexEntry...)
		}
	}
	return dataBlocks, indexBlock, bloom, kr, nil
}

func encodeIndexEntry(entry *indexEntry) (data []byte) {
	data = append(data, uint8(len(entry.key)))
	data = append(data, []byte(entry.key)...)
	blockBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockBytes, entry.block)
	data = append(data, blockBytes...)
	return data
}
