package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// NewSSTFile adds new SST file to in-memory manifest and sends update request to manifest channel
func (level *level) NewSSTFile(fileID string, keyRange *keyRange, bloom *bloom) {
	level.manifestLock.Lock()
	level.manifest[fileID] = keyRange
	level.manifestLock.Unlock()

	level.bloomLock.Lock()
	level.blooms[fileID] = bloom
	level.bloomLock.Unlock()

	if level.level == 0 && len(level.manifest)-len(level.merging) > compactThreshold {
		compact := level.mergeManifest()
		level.below.compactReqChan <- compact
	}
}

// FindSSTFile finds files in level where key falls in their key range
func (level *level) FindSSTFile(key string) (filenames []string) {
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
func (level *level) RangeSSTFiles(startKey, endKey string) (filenames []string) {
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
func (level *level) DeleteSSTFiles(files []string) error {
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
		err := os.RemoveAll(file)
		if err != nil {
			return err
		}
	}

	return nil
}

func (level *level) mergeManifest() []*merge {
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

func (level *level) printManifest() {
	type manifest struct {
		fileID   string
		keyRange *keyRange
	}
	mfsts := []*manifest{}
	for fileID, keyRange := range level.manifest {
		mfsts = append(mfsts, &manifest{fileID: fileID, keyRange: keyRange})
	}

	sort.Slice(mfsts, func(i, j int) bool {
		return mfsts[i].keyRange.startKey < mfsts[j].keyRange.endKey
	})

	for _, mfst := range mfsts {
		fmt.Println(level.level, mfst.keyRange.startKey, mfst.keyRange.endKey)
	}
}
