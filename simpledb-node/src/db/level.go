package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// KeyRange represents struct for range of keys
type KeyRange struct {
	startKey string
	endKey   string
}

type merge struct {
	files    []string
	keyRange *KeyRange
}

type compactReply struct {
	files []string
	err   error
}

// Level represents struct for level in LSM tree
type Level struct {
	level     int
	capacity  int
	size      int
	directory string

	merging   map[string]struct{}
	mergeLock sync.RWMutex

	manifest     map[string]*KeyRange
	manifestSync map[string]*KeyRange
	manifestLock sync.RWMutex

	blooms    map[string]*Bloom
	bloomLock sync.RWMutex

	compactReqChan   chan []*merge
	compactReplyChan chan *compactReply

	above *Level
	below *Level

	close chan struct{}
}

// NewLevel creates a new level in the LSM tree
func NewLevel(level, capacity int, directory string) (*Level, error) {
	err := os.MkdirAll(filepath.Join(directory, "L"+strconv.Itoa(level)), os.ModePerm)
	if err != nil {
		return nil, err
	}

	lvl := &Level{
		level:     level,
		capacity:  capacity,
		size:      0,
		directory: filepath.Join(directory, "L"+strconv.Itoa(level)),

		merging: make(map[string]struct{}),

		manifest:     make(map[string]*KeyRange),
		manifestSync: make(map[string]*KeyRange),

		blooms: make(map[string]*Bloom),

		compactReqChan:   make(chan []*merge, 16),
		compactReplyChan: make(chan *compactReply, 16),

		above: nil,
		below: nil,

		close: make(chan struct{}),
	}

	err = lvl.RecoverLevel()
	if err != nil {
		return nil, err
	}

	go lvl.run()

	return lvl, nil
}

func (level *Level) getUniqueID() string {
	level.manifestLock.RLock()
	defer level.manifestLock.RUnlock()

	id := uuid.New().String()[:8]
	if _, ok := level.manifest[id]; !ok {
		return id
	}
	return level.getUniqueID()
}

// Merge takes an empty or existing file at the current level and merges it with file(s) from the level above
func (level *Level) Merge(files []string) {
	if len(files) == 1 {
		file := files[0]
		info, err := os.Stat(file)
		if err != nil {
			level.compactReplyChan <- &compactReply{files: nil, err: err}
			return
		}

		size := int(info.Size())

		fileArr := strings.Split(file, "/")
		oldFileID := strings.Split(fileArr[len(fileArr)-1], ".")[0]
		newFileID := level.getUniqueID()

		err = os.Rename(file, filepath.Join(level.directory, newFileID+".sst"))
		if err != nil {
			level.compactReplyChan <- &compactReply{files: nil, err: err}
			return
		}

		level.above.manifestLock.Lock()
		level.above.bloomLock.Lock()
		level.above.mergeLock.Lock()
		keyRange := level.above.manifest[oldFileID]
		bloom := level.above.blooms[oldFileID]
		delete(level.above.manifest, oldFileID)
		delete(level.above.blooms, oldFileID)
		delete(level.above.merging, oldFileID)
		level.above.manifestLock.Unlock()
		level.above.bloomLock.Unlock()
		level.above.mergeLock.Unlock()

		level.NewSSTFile(newFileID, keyRange, bloom)
		level.size += size
		return
	}

	entries, err := mergeSort(files)
	if err != nil {
		level.compactReplyChan <- &compactReply{files: nil, err: err}
		return
	}

	err = level.writeMerge(entries)
	if err != nil {
		level.compactReplyChan <- &compactReply{files: nil, err: err}
		return
	}

	aboveFiles := []string{}
	belowFiles := []string{}

	for _, file := range files {
		if strings.Contains(file, "L"+strconv.Itoa(level.level)) {
			belowFiles = append(belowFiles, file)
		} else {
			aboveFiles = append(aboveFiles, file)
		}
	}

	level.above.compactReplyChan <- &compactReply{files: aboveFiles, err: nil}
	level.compactReplyChan <- &compactReply{files: belowFiles, err: nil}
}

func (level *Level) writeMerge(entries []*LSMDataEntry) error {
	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		return err
	}

	keyRangeEntry := createKeyRangeEntry(keyRange)
	header := createHeader(len(dataBlocks), len(indexBlock), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(dataBlocks, indexBlock...), bloom.bits...), keyRangeEntry...)...)

	fileID := level.getUniqueID()
	filename := filepath.Join(level.directory, fileID+".sst")

	err = writeNewFile(filename, data)
	if err != nil {
		return err
	}

	level.NewSSTFile(fileID, keyRange, bloom)
	level.size += len(data)

	return nil
}

// Range gets all files at a specific level whose key range fall within the given range query.
// It then concurrently reads all files and returns the result to the given channel
func (level *Level) Range(startKey, endKey string, replyChan chan []*LSMDataEntry, errChan chan error) {
	filenames := level.RangeSSTFiles(startKey, endKey)
	replies := make(chan []*LSMDataEntry)
	errs := make(chan error)
	result := []*LSMDataEntry{}
	var resultErr error
	var wg sync.WaitGroup

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go func(filename string) {
			fileRangeQuery(filename, startKey, endKey, replies, errs)
		}(filename)
	}

	go func() {
		for {
			select {
			case reply := <-replies:
				result = append(result, reply...)
				wg.Done()
			case err := <-errs:
				resultErr = err
				wg.Done()
			}
		}
	}()

	wg.Wait()

	if resultErr != nil {
		errChan <- resultErr
		return
	}
	replyChan <- result
}

// RecoverLevel reads all files at a level's directory and updates all necessary in-memory data for the level.
// In particular, it updates the level's total size, manifest, and bloom filters.
func (level *Level) RecoverLevel() error {
	entries, err := ioutil.ReadDir(level.directory)
	if err != nil {
		return err
	}

	filenames := make(map[string]string)

	for _, fileInfo := range entries {
		filename := fileInfo.Name()
		if strings.HasSuffix(filename, ".sst") {
			fileID := strings.Split(filename, ".")[0]
			filenames[fileID] = filepath.Join(level.directory, filename)
		}
	}

	for fileID, filename := range filenames {
		entries, bloom, size, err := RecoverFile(filename)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			fmt.Println(filename, len(entries))
			startKey := entries[0].key
			endKey := entries[len(entries)-1].key
			level.manifest[fileID] = &KeyRange{startKey: startKey, endKey: endKey}
			level.blooms[fileID] = bloom
			level.size += size
		}
	}

	return nil
}

// Close closes all level's operations including merging, compacting, and adding SST files.
func (level *Level) Close() {
	level.close <- struct{}{}
}

func (level *Level) run() {
	exceedCapacity := time.NewTicker(1 * time.Second)

	for {
		select {
		case compact := <-level.compactReqChan:
			var merged bool
			level.manifestLock.RLock()
			level.mergeLock.Lock()
			for fileID, keyRange := range level.manifest {
				if _, ok := level.merging[fileID]; !ok {
					merge := &merge{
						files:    []string{filepath.Join(level.directory, fileID+".sst")},
						keyRange: keyRange,
					}
					merged, compact = mergeInterval(compact, merge)
					if merged {
						level.merging[fileID] = struct{}{}
					}
				}
			}
			level.manifestLock.RUnlock()
			level.mergeLock.Unlock()

			var wg sync.WaitGroup

			for _, merge := range compact {
				wg.Add(1)
				go func(files []string) {
					defer wg.Done()
					level.Merge(files)
				}(merge.files)
			}

			wg.Wait()
		case reply := <-level.compactReplyChan:
			if reply.err != nil {
				fmt.Println(reply.err)
			} else {
				err := level.DeleteSSTFiles(reply.files)
				if err != nil {
					fmt.Println(err)
				}
			}
		case <-exceedCapacity.C:
			if level.size > level.capacity && level.below != nil {
				level.size = 0
				compact := level.mergeManifest()
				level.below.compactReqChan <- compact
			}
		case <-level.close:
			return
		}
	}
}
