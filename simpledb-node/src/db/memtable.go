package db

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// memTable is struct for Write-Ahead-Log and memtable
type memTable struct {
	table *avlTree
	wal   string
	size  int
}

// newMemTable creates a file for the WAL and a new Memtable
func newMemTable(directory string, id string) (mt *memTable, maxCommitTs uint64, err error) {
	err = os.MkdirAll(filepath.Join(directory, "memtables"), dirPerm)
	if err != nil {
		return nil, 0, err
	}

	filename := "WAL_" + id

	mt = &memTable{
		table: newAVLTree(),
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

// Write first appends a batch of writes to WAL then inserts them all into in-memory table
func (mt *memTable) Write(entries []*Entry) error {
	data := []byte{}
	for _, entry := range entries {
		data = append(data, encodeEntry(entry)...)
	}
	err := mt.AppendWAL(data)
	if err != nil {
		return err
	}
	// Put entries into memory structure after append to WAL to ensure consistency
	for _, entry := range entries {
		mt.table.Put(entry)
	}
	mt.size += len(data)
	return nil
}

// AppendWAL encodes an lsmDataEntry into bytes and appends to the WAL
func (mt *memTable) AppendWAL(data []byte) error {
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
		return newErrWriteUnexpectedBytes(mt.wal)
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

// RecoverWAL reads the WAL and repopulates the memtable
func (mt *memTable) RecoverWAL() (maxCommitTs uint64, err error) {
	data, err := ioutil.ReadFile(mt.wal)
	if err != nil {
		return 0, err
	}
	mt.size = len(data)
	entries := []*Entry{}
	i := 0
	for i < len(data) {
		if i+4 > len(data) {
			break
		}
		entrySize := binary.LittleEndian.Uint32(data[i : i+4])
		i += 4
		if i+int(entrySize) > len(data) {
			return 0, newErrBadFormattedSST()
		}
		if entrySize == 0 {
			break
		}
		entry, err := decodeEntry(data[i : i+int(entrySize)])
		if err != nil {
			return 0, err
		}
		i += int(entrySize)
		entries = append(entries, entry)
	}
	for _, entry := range entries {
		mt.table.Put(entry)
		if entry.ts > maxCommitTs {
			maxCommitTs = entry.ts
		}
	}
	for _, entry := range mt.table.Inorder() {
		fmt.Println(entry)
	}
	return maxCommitTs, nil
}
