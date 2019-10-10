package db

import (
	"errors"
	"os"
	"strconv"

	"github.com/google/uuid"
)

type keyRange struct {
	startKey string
	endKey   string
}

type Level struct {
	level    int
	capacity int
	manifest map[string]*keyRange
}

func NewLevel(level int) (*Level, error) {
	f, err := os.OpenFile("L0.manifest", os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	return &Level{
		level:    level,
		capacity: blockSize,
		manifest: make(map[string]*keyRange),
	}, nil
}

func (level *Level) NewSSTFile(filename, startKey, endKey string) error {
	f, err := os.OpenFile("L"+strconv.Itoa(level.level)+".manifest_new", os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()

	if err != nil {
		return err
	}

	data := make([]byte, filenameLength+keySize*2)

	for filename, item := range level.manifest {
		startKeyBytes := make([]byte, keySize)
		endKeyBytes := make([]byte, keySize)

		copy(startKeyBytes[0:], []byte(item.startKey))
		copy(endKeyBytes[0:], []byte(item.endKey))

		copy(data[0:], []byte(filename))
		copy(data[filenameLength:], startKeyBytes)
		copy(data[filenameLength+keySize:], endKeyBytes)
	}

	numBytes, err := f.Write(data[:])
	if err != nil {
		return err
	}

	if numBytes != filenameLength+keySize*2 {
		return errors.New("Num bytes written to manifest does not match data")
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	err = os.Rename("L"+strconv.Itoa(level.level)+".manifest_new", "L"+strconv.Itoa(level.level)+".manifest")
	if err != nil {
		return err
	}

	level.manifest[filename] = &keyRange{startKey: startKey, endKey: endKey}
	return nil
}

func (level *Level) FindSSTFile(key string) (filenames []string) {
	for filename, item := range level.manifest {
		if key > item.startKey && key < item.endKey {
			filenames = append(filenames, filename)
		}
	}
	return filenames
}

func (level *Level) getUniqueId() string {
	id := uuid.New().String()[:8]
	if _, ok := level.manifest[id]; !ok {
		return id
	}
	return level.getUniqueId()
}
