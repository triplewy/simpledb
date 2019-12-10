package db

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// keyRange represents struct for range of keys
type keyRange struct {
	startKey string
	endKey   string
}

type merge struct {
	files    []string
	keyRange *keyRange
}

type compactReply struct {
	files []string
	err   error
}

// level represents struct for level in lsm tree
type level struct {
	level     int
	capacity  int
	size      int
	directory string

	merging   map[string]struct{}
	mergeLock sync.RWMutex

	manifest     map[string]*keyRange
	manifestSync map[string]*keyRange
	manifestLock sync.RWMutex

	blooms    map[string]*bloom
	bloomLock sync.RWMutex

	compactReqChan   chan []*merge
	compactReplyChan chan []string

	above *level
	below *level

	fm *fileManager

	close chan struct{}
}

// newLevel creates a new level in the lsm tree
func newLevel(numLevel int, directory string, fm *fileManager) (*level, error) {
	err := os.MkdirAll(filepath.Join(directory, "L"+strconv.Itoa(numLevel)), dirPerm)
	if err != nil {
		return nil, err
	}

	capacity := 0
	if numLevel == 0 {
		capacity = 2 * MemTableSize
	} else {
		capacity = int(math.Pow10(numLevel)) * multiplier
	}

	lvl := &level{
		level:     numLevel,
		capacity:  capacity,
		size:      0,
		directory: filepath.Join(directory, "L"+strconv.Itoa(numLevel)),

		merging: make(map[string]struct{}),

		manifest:     make(map[string]*keyRange),
		manifestSync: make(map[string]*keyRange),

		blooms: make(map[string]*bloom),

		compactReqChan:   make(chan []*merge, 16),
		compactReplyChan: make(chan []string, 16),

		above: nil,
		below: nil,

		fm: fm,

		close: make(chan struct{}),
	}
	go lvl.run()

	err = lvl.Recoverlevel()
	if err != nil {
		return nil, err
	}

	return lvl, nil
}

func (level *level) Find(key string, ts uint64) (*Entry, error) {
	filenames := level.FindSSTFile(key)
	if len(filenames) == 0 {
		return nil, newErrKeyNotFound()
	}
	if len(filenames) == 1 {
		return level.fm.Find(filenames[0], key, ts)
	}

	replyChan := make(chan *Entry)
	errChan := make(chan error)
	replies := []*Entry{}

	var wg sync.WaitGroup
	var errs []error

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go func(filename string) {
			entry, err := level.fm.Find(filename, key, ts)
			if err != nil {
				errChan <- err
			} else {
				replyChan <- entry
			}
		}(filename)
	}

	go func() {
		for {
			select {
			case reply := <-replyChan:
				replies = append(replies, reply)
				wg.Done()
			case err := <-errChan:
				errs = append(errs, err)
				wg.Done()
			}
		}
	}()
	wg.Wait()

	for _, err := range errs {
		switch err.(type) {
		case *ErrKeyNotFound:
			continue
		case *os.PathError:
			// If encounter race condition of non existent file, make sure to delete it from manifest
			fmt.Println(key, err)
			filename := strings.Fields(err.Error())[1]
			filename = filename[:len(filename)-1]
			err := level.DeleteSSTFiles([]string{filename})
			if err != nil {
				fmt.Println(err)
			}
			return level.Find(key, ts)
		default:
			return nil, err
		}
	}

	if len(replies) > 0 {
		var latestUpdate *Entry
		for _, entry := range replies {
			if latestUpdate == nil {
				latestUpdate = entry
			} else {
				if entry.ts > latestUpdate.ts {
					latestUpdate = entry
				}
			}
		}
		return latestUpdate, nil
	}
	return nil, newErrKeyNotFound()
}

func (level *level) getUniqueID() string {
	level.manifestLock.RLock()
	defer level.manifestLock.RUnlock()

	id := uuid.New().String()[:8]
	if _, ok := level.manifest[id]; !ok {
		return id
	}
	return level.getUniqueID()
}

// Merge takes an empty or existing file at the current level and merges it with file(s) from the level above
func (level *level) Merge(files []string) ([]string, error) {
	if len(files) == 1 {
		file := files[0]
		info, err := os.Stat(file)
		if err != nil {
			return nil, err
		}
		size := int(info.Size())
		fileArr := strings.Split(file, "/")
		oldFileID := strings.Split(fileArr[len(fileArr)-1], ".")[0]
		newFileID := level.getUniqueID()

		// Get key range and bloom filter
		level.above.manifestLock.RLock()
		level.above.bloomLock.RLock()
		keyRange := level.above.manifest[oldFileID]
		bloom := level.above.blooms[oldFileID]
		level.above.manifestLock.RUnlock()
		level.above.bloomLock.RUnlock()

		level.NewSSTFile(newFileID, keyRange, bloom)
		level.size += size
		err = os.Rename(file, filepath.Join(level.directory, newFileID+".sst"))
		if err != nil {
			level.manifestLock.Lock()
			delete(level.manifest, newFileID)
			level.manifestLock.Unlock()
			level.size -= size
			return nil, err
		}
		// Delete old key range and bloom filter
		level.above.manifestLock.Lock()
		level.above.mergeLock.Lock()
		level.above.bloomLock.Lock()
		delete(level.above.manifest, oldFileID)
		delete(level.above.blooms, oldFileID)
		delete(level.above.merging, oldFileID)
		level.above.manifestLock.Unlock()
		level.above.mergeLock.Unlock()
		level.above.bloomLock.Unlock()
		return nil, nil
	}

	entries, err := level.mergeSort(files)
	if err != nil {
		return nil, err
	}
	err = level.writeMerge(entries)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (level *level) writeMerge(entries []*Entry) error {
	dataBlocks, indexBlock, bloom, keyRange, err := writeEntries(entries)
	if err != nil {
		return err
	}

	keyRangeEntry := createkeyRangeEntry(keyRange)
	header := createHeader(len(dataBlocks), len(indexBlock), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(dataBlocks, indexBlock...), bloom.bits...), keyRangeEntry...)...)

	fileID := level.getUniqueID()
	filename := filepath.Join(level.directory, fileID+".sst")

	err = level.fm.Write(filename, data)
	if err != nil {
		return err
	}
	level.NewSSTFile(fileID, keyRange, bloom)
	level.size += len(data)

	return nil
}

// Range gets all files at a specific level whose key range fall within the given range query.
// It then concurrently reads all files and returns the result to the given channel
func (level *level) Range(keyRange *keyRange, ts uint64) (entries []*Entry, err error) {
	filenames := level.RangeSSTFiles(keyRange.startKey, keyRange.endKey)
	replyChan := make(chan []*Entry)
	errChan := make(chan error)
	errs := make(map[string]int)
	var wg sync.WaitGroup

	wg.Add(len(filenames))
	for _, filename := range filenames {
		go func(filename string) {
			entries, err := level.fm.Range(filename, keyRange, ts)
			if err != nil {
				errChan <- err
			} else {
				replyChan <- entries
			}
		}(filename)
	}

	go func() {
		for {
			select {
			case reply := <-replyChan:
				entries = append(entries, reply...)
				wg.Done()
			case err := <-errChan:
				if _, ok := errs[err.Error()]; !ok {
					errs[err.Error()] = 1
				} else {
					errs[err.Error()]++
				}
				wg.Done()
			}
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return entries, fmt.Errorf("Errors during range query on level %d: %v", level.level, errs)
	}
	return entries, nil
}

// Recoverlevel reads all files at a level's directory and updates all necessary in-memory data for the level.
// In particular, it updates the level's total size, manifest, and bloom filters.
func (level *level) Recoverlevel() error {
	entries, err := ioutil.ReadDir(level.directory)
	if err != nil {
		return err
	}

	filenames := make(map[string]string)

	for _, fileInfo := range entries {
		filename := fileInfo.Name()
		fileID := strings.Split(filename, ".")[0]
		filenames[fileID] = filepath.Join(level.directory, filename)
	}

	for fileID, filename := range filenames {
		keyRange, bloom, size, err := recoverFile(filename)
		if err != nil {
			return err
		}
		level.NewSSTFile(fileID, keyRange, bloom)
		level.size += size
	}

	return nil
}

// Close closes all level's operations including merging, compacting, and adding SST files.
func (level *level) Close() {
	level.close <- struct{}{}
}

func (level *level) run() {
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
					removeFiles, err := level.Merge(files)
					if err != nil {
						fmt.Println(err)
					} else {
						aboveFiles := []string{}
						belowFiles := []string{}
						for _, file := range removeFiles {
							if strings.Contains(file, "L"+strconv.Itoa(level.level)) {
								belowFiles = append(belowFiles, file)
							} else {
								aboveFiles = append(aboveFiles, file)
							}
						}
						level.above.compactReplyChan <- aboveFiles
						level.compactReplyChan <- belowFiles
					}
				}(merge.files)
			}
			wg.Wait()
		case files := <-level.compactReplyChan:
			err := level.DeleteSSTFiles(files)
			if err != nil {
				fmt.Println(err)
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
