package db

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// NewSSTFile adds new SST file to in-memory manifest and sends update request to manifest channel
func (level *Level) NewSSTFile(fileID, startKey, endKey string, bloom *Bloom) {
	level.manifestLock.Lock()
	level.manifest[fileID] = &KeyRange{startKey: startKey, endKey: endKey}
	level.manifestLock.Unlock()

	level.bloomLock.Lock()
	level.blooms[fileID] = bloom
	level.bloomLock.Unlock()

	if level.level == 0 && len(level.manifest)%compactThreshold == 0 {
		compact := level.mergeManifest()
		level.below.compactReqChan <- compact
	}
}

// FindSSTFile finds files in level where key falls in their key range
func (level *Level) FindSSTFile(key string) (filenames []string) {
	level.manifestLock.RLock()
	level.bloomLock.RLock()
	defer level.manifestLock.RUnlock()
	defer level.bloomLock.RUnlock()

	for filename, item := range level.manifest {
		if item.startKey <= key && key <= item.endKey {
			if level.blooms[filename].Check(key) {
				filenames = append(filenames, filepath.Join(level.directory, filename+".sst"))
			}
		}
	}
	return filenames
}

// RangeSSTFiles finds files in level where their key range falls in the range query
func (level *Level) RangeSSTFiles(startKey, endKey string) (filenames []string) {
	level.manifestLock.RLock()
	defer level.manifestLock.RUnlock()

	for filename, item := range level.manifest {
		if (item.startKey <= startKey && startKey <= item.endKey) || (item.startKey <= endKey && endKey <= item.endKey) || (startKey <= item.startKey && endKey >= item.endKey) {
			filenames = append(filenames, filepath.Join(level.directory, filename+".sst"))
		}
	}
	return filenames
}

// DeleteSSTFiles deletes SST files and updates in-memory manifest
func (level *Level) DeleteSSTFiles(files []string) error {
	level.manifestLock.Lock()
	level.mergeLock.Lock()
	level.bloomLock.Lock()

	for _, file := range files {
		arr := strings.Split(file, "/")
		id := strings.Split(arr[len(arr)-1], ".")[0]

		delete(level.manifest, id)
		delete(level.merging, id)
		delete(level.blooms, id)
	}

	level.manifestLock.Unlock()
	level.mergeLock.Unlock()
	level.bloomLock.Unlock()

	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateManifest writes in-memory manifest to disk
func (level *Level) UpdateManifest() error {
	level.manifestLock.RLock()
	defer level.manifestLock.RUnlock()

	hasUpdated := false
	for filename, item1 := range level.manifest {
		if item2, ok := level.manifestSync[filename]; !ok {
			level.manifestSync[filename] = item1
			hasUpdated = true
		} else {
			if item1.startKey != item2.startKey || item1.endKey != item2.endKey {
				level.manifestSync[filename] = item1
				hasUpdated = true
			}
		}
	}

	for filename := range level.manifestSync {
		if _, ok := level.manifest[filename]; !ok {
			delete(level.manifestSync, filename)
			hasUpdated = true
		}
	}

	if !hasUpdated {
		return nil
	}

	manifest := []byte{}

	for filename, item := range level.manifest {
		data := []byte{}
		data = append(data, []byte(filename)...)
		data = append(data, uint8(len(item.startKey)))
		data = append(data, []byte(item.startKey)...)
		data = append(data, uint8(len(item.endKey)))
		data = append(data, []byte(item.endKey)...)

		manifest = append(manifest, data...)
	}

	err := writeNewFile(filepath.Join(level.directory, "manifest_new"), manifest)
	if err != nil {
		return err
	}

	err = os.Rename(filepath.Join(level.directory, "manifest_new"), filepath.Join(level.directory, "manifest"))
	if err != nil {
		return err
	}

	return nil
}

// ReadManifest reads manifest file and updates in-memory manifest
func (level *Level) ReadManifest(filename string) (map[string]*KeyRange, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	manifest := make(map[string]*KeyRange)
	i := 0
	for i < len(data) {
		fileID := string(data[i : i+8])
		i += 8
		startKeyLength := uint8(data[i])
		i++
		startKey := string(data[i : i+int(startKeyLength)])
		i += int(startKeyLength)
		endKeyLength := uint8(data[i])
		i++
		endKey := string(data[i : i+int(endKeyLength)])
		i += int(endKeyLength)

		manifest[fileID] = &KeyRange{startKey: startKey, endKey: endKey}
	}

	return manifest, nil
}

func (level *Level) mergeManifest() []*merge {
	level.mergeLock.Lock()
	level.manifestLock.RLock()
	defer level.mergeLock.Unlock()
	defer level.manifestLock.RUnlock()

	compact := []*merge{}

	for fileID, keyRange := range level.manifest {
		if _, ok := level.merging[fileID]; !ok {
			merge := &merge{
				files:    []string{filepath.Join(level.directory, fileID+".sst")},
				keyRange: keyRange,
			}
			compact = append(compact, merge)
			level.merging[fileID] = struct{}{}
		}
	}
	return mergeIntervals(compact)
}
