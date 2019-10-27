package db

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// RecoverLevel reads all files at a level's directory and updates all necessary in-memory data for the level.
// In particular, it updates the level's total size, manifest, and bloom filters.
func (level *Level) RecoverLevel() error {
	entries, err := ioutil.ReadDir(level.directory)
	if err != nil {
		return err
	}

	foundManifestNew := false
	manifest := make(map[string]*KeyRange)
	filenames := make(map[string]string)

	for _, fileInfo := range entries {
		filename := fileInfo.Name()
		if strings.HasSuffix(filename, ".sst") {
			fmt.Println(filename)
			fileID := strings.Split(filename, ".")[0]
			filenames[fileID] = filepath.Join(level.directory, filename)
		} else if filename == "manifest" && !foundManifestNew {
			manifest, err = level.ReadManifest(filename)
			if err != nil {
				return err
			}
		} else if filename == "manifest_new" {
			manifest, err = level.ReadManifest(filename)
			if err != nil {
				return err
			}
			foundManifestNew = true
		}
	}

	// Delete missing files from manifest
	for fileID := range manifest {
		if _, ok := filenames[fileID]; !ok {
			delete(manifest, fileID)
		}
	}

	totalSize := 0
	blooms := make(map[string]*Bloom)
	// Add files that are not in manifest
	for fileID, filename := range filenames {
		if _, ok := manifest[fileID]; !ok {
			entries, bloom, size, err := RecoverFile(filename)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				startKey := entries[0].key
				endKey := entries[len(entries)-1].key
				manifest[fileID] = &KeyRange{startKey: startKey, endKey: endKey}
				// Only add to blooms map if file has any entries in the first place
				blooms[fileID] = bloom
				// Only add size if file has any entries
				totalSize += size
			}
		}
	}

	level.manifest = manifest
	level.blooms = blooms
	level.size = totalSize
	return nil
}

func (db *DB) recoverFlush() (uint64, error) {
	f, err := os.OpenFile(db.flushWAL, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return 0, err
	}

	info, err := os.Stat(db.flushWAL)
	if err != nil {
		return 0, err
	}

	size := int64(info.Size())

	offsetBytes := make([]byte, 8)

	numBytes, err := f.ReadAt(offsetBytes, size-8)
	if err != nil {
		return 0, err
	}
	if numBytes != len(offsetBytes) {
		return 0, newErrReadUnexpectedBytes(db.flushWAL)
	}

	offset := binary.LittleEndian.Uint64(offsetBytes)

	return offset, nil
}

// RecoverMemTables first reads flush WAL to determine last flushed offset. Then read value log
// from the offset until the end of the value log. For each entry in this data buffer, we insert
// into the mutable memtable
func (db *DB) RecoverMemTables() error {
	offset, err := db.recoverFlush()
	if err != nil {
		return err
	}

	f, err := os.OpenFile(db.vlog.fileName, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	info, err := os.Stat(db.vlog.fileName)
	if err != nil {
		return err
	}

	size := int64(info.Size())

	data := make([]byte, size-int64(offset))

	numBytes, err := f.ReadAt(data, int64(offset))
	if err != nil {
		return err
	}
	if numBytes != len(data) {
		return newErrReadUnexpectedBytes(db.vlog.fileName)
	}

	i := 0
	for i < len(data) {
		vlogOffset := offset + uint64(i)
		keySize := uint8(data[i])
		i++
		key := string(data[i : i+int(keySize)])
		i += int(keySize)
		valueSize := binary.LittleEndian.Uint32(data[i : i+4])
		i += 4
		value := string(data[i : i+int(valueSize)])
		entry := &LSMDataEntry{
			keySize:    keySize,
			key:        key,
			vlogOffset: vlogOffset,
			vlogSize:   uint32(keySize) + valueSize + 5,
		}
		err := db.mutable.Put(key, value, entry)
		if err != nil {
			return err
		}
	}
	return nil
}
