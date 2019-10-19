package db

import (
	"errors"
	"os"
	"strings"
)

// NewSSTFile adds new SST file to in-memory manifest and sends update request to manifest channel
func (level *Level) NewSSTFile(filename, startKey, endKey string) {
	level.manifestLock.Lock()
	level.manifest[filename] = &keyRange{startKey: startKey, endKey: endKey}
	level.manifestLock.Unlock()

	if level.level == 0 && len(level.manifest)%compactThreshold == 0 {
		compact := level.mergeManifest()
		level.below.compactReqChan <- compact
	}
}

// FindSSTFile finds files in level where key falls in their key range
func (level *Level) FindSSTFile(key string) (filenames []string) {
	level.manifestLock.RLock()
	defer level.manifestLock.RUnlock()

	for filename, item := range level.manifest {
		if key >= item.startKey && key <= item.endKey {
			filenames = append(filenames, level.directory+filename+".sst")
		}
	}
	return filenames
}

// DeleteSSTFiles deletes SST files and updates in-memory manifest
func (level *Level) DeleteSSTFiles(files []string) error {
	level.manifestLock.Lock()
	level.mergeLock.Lock()

	for _, file := range files {
		arr := strings.Split(file, "/")
		id := strings.Split(arr[len(arr)-1], ".")[0]

		delete(level.manifest, id)
		delete(level.merging, id)
	}

	level.manifestLock.Unlock()
	level.mergeLock.Unlock()

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

	if !hasUpdated {
		return nil
	}

	f, err := os.OpenFile(level.directory+"manifest_new", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	manifest := []byte{}

	for filename, item := range level.manifest {
		data := make([]byte, filenameLength+keySize*2+2)
		startKeyBytes := make([]byte, keySize)
		endKeyBytes := make([]byte, keySize)

		copy(startKeyBytes[0:], []byte(item.startKey))
		copy(endKeyBytes[0:], []byte(item.endKey))

		copy(data[0:], []byte(filename))
		copy(data[filenameLength:], startKeyBytes)
		copy(data[filenameLength+keySize:], endKeyBytes)
		copy(data[len(data)-2:], []byte("\r\n"))
		manifest = append(manifest, data[:]...)
	}

	numBytes, err := f.Write(manifest)
	if err != nil {
		return err
	}

	if numBytes != len(level.manifest)*(filenameLength+keySize*2+2) {
		return errors.New("Num bytes written to manifest does not match data")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	err = os.Rename(level.directory+"manifest_new", level.directory+"manifest")
	if err != nil {
		return err
	}

	return nil
}

func (level *Level) mergeManifest() []*merge {
	level.manifestLock.RLock()
	level.mergeLock.Lock()
	defer level.manifestLock.RUnlock()
	defer level.mergeLock.Unlock()

	compact := []*merge{}

	for filename, keyRange := range level.manifest {
		if _, ok := level.merging[filename]; !ok {
			sk1 := keyRange.startKey
			ek1 := keyRange.endKey
			merged := false

			for i, mergeStruct := range compact {
				sk2 := mergeStruct.keyRange.startKey
				ek2 := mergeStruct.keyRange.endKey

				if (sk1 >= sk2 && sk1 <= ek2) || (ek1 >= sk2 && ek1 <= ek2) {
					compact[i].aboveFiles = append(compact[i].aboveFiles, level.directory+filename+".sst")

					if sk1 < sk2 {
						compact[i].keyRange.startKey = sk1
					}
					if ek1 > ek2 {
						compact[i].keyRange.endKey = ek1
					}
					merged = true
					break
				}
			}

			if !merged {
				compact = append(compact, &merge{
					aboveFiles: []string{level.directory + filename + ".sst"},
					belowFile:  "",
					keyRange:   keyRange,
				})
			}

			level.merging[filename] = struct{}{}
		}
	}

	return compact
}
