package db

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// MemTable is struct for Write-Ahead-Log and memtable
type MemTable struct {
	table *AVLTree
	wal   string
	size  int
}

// NewMemTable creates a file for the WAL and a new Memtable
func NewMemTable(directory string, id string) (mt *MemTable, maxCommitTs uint64, err error) {
	err = os.MkdirAll(filepath.Join(directory, "memtables"), os.ModePerm)
	if err != nil {
		return nil, 0, err
	}

	filename := "WAL_" + id

	mt = &MemTable{
		table: NewAVLTree(),
		wal:   filepath.Join(directory, "memtables", filename),
		size:  0,
	}

	f, err := os.OpenFile(mt.wal, os.O_CREATE|os.O_EXCL, os.ModePerm)
	defer f.Close()
	if err != nil {
		if !strings.HasSuffix(err.Error(), "file exists") {
			return nil, 0, err
		}
		maxCommitTs, err = mt.RecoverWAL()
		if err != nil {
			return nil, 0, err
		}
	}
	return mt, maxCommitTs, nil
}

// Put first appends to WAL then inserts into the in-memory table
func (mt *MemTable) Put(entry *LSMDataEntry) error {
	data := encodeDataEntry(entry)
	err := mt.AppendWAL(data)
	if err != nil {
		return err
	}
	mt.table.Put(entry)
	mt.size += sizeDataEntry(entry)
	return nil
}

// BatchPut first appends a batch of writes to WAL then inserts them all into in-memory table
func (mt *MemTable) BatchPut(entries []*LSMDataEntry) error {
	data := []byte{}
	for _, entry := range entries {
		data = append(data, encodeDataEntry(entry)...)
	}
	err := mt.AppendWAL(data)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		mt.table.Put(entry)
		mt.size += sizeDataEntry(entry)
	}
	return nil
}

// Get searches in-memory table for key
func (mt *MemTable) Get(key string) *LSMDataEntry {
	node := mt.table.Find(key)
	if node != nil {
		return mt.table.Find(key).entry
	}
	return nil
}

// Range searches in-memory table for keys within key range
func (mt *MemTable) Range(startKey, endKey string) []*LSMDataEntry {
	return mt.table.Range(startKey, endKey)
}

// AppendWAL encodes an LSMDataEntry into bytes and appends to the WAL
func (mt *MemTable) AppendWAL(data []byte) error {
	f, err := os.OpenFile(mt.wal, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		return err
	}
	numBytes, err := f.Write(data)
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return ErrWriteUnexpectedBytes(mt.wal)
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

// RecoverWAL reads the WAL and repopulates the memtable
func (mt *MemTable) RecoverWAL() (maxCommitTs uint64, err error) {
	data, err := ioutil.ReadFile(mt.wal)
	if err != nil {
		return 0, err
	}
	i := 0
	for i < len(data) {
		seqID := binary.LittleEndian.Uint64(data[i : i+8])
		if seqID > maxCommitTs {
			maxCommitTs = seqID
		}
		i += 8
		keySize := uint8(data[i])
		i++
		key := string(data[i : i+int(keySize)])
		i += int(keySize)
		valueType := uint8(data[i])
		i++
		valueSize := binary.LittleEndian.Uint16(data[i : i+2])
		i += 2
		value := data[i : i+int(valueSize)]
		i += int(valueSize)
		entry := &LSMDataEntry{
			seqID:     seqID,
			keySize:   keySize,
			key:       key,
			valueType: valueType,
			valueSize: valueSize,
			value:     value,
		}
		mt.table.Put(entry)
		mt.size += sizeDataEntry(entry)
	}
	return maxCommitTs, nil
}
