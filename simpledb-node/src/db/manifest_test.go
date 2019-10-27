package db

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestManifestRead(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	db, err := NewDB("data")
	if err != nil {
		t.Fatalf("Error creating DB: %v\n", err)
	}

	numItems := 50000

	startInsertTime := time.Now()
	for i := 0; i < numItems; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		err = db.Put(key, key)
		if err != nil {
			t.Fatalf("Error Putting into LSM: %v\n", err)
		}
	}
	duration := time.Since(startInsertTime)
	fmt.Printf("Duration inserting %d items: %v\n", numItems, duration)
	fmt.Printf("Duration opening vlog: %v\n", db.vlog.openTime)

	time.Sleep(5 * time.Second)

	for _, level := range db.lsm.levels {
		if len(level.manifest) > 0 {
			manifest, err := level.ReadManifest(filepath.Join(level.directory, "manifest"))
			if err != nil {
				t.Fatalf("Error reading manifest: %v\n", err)
			}
			if len(manifest) != len(level.manifest) {
				t.Fatalf("Level: %d, Length of read manifest: %d does not match length of in-memory manifest: %d\n", level.level, len(manifest), len(level.manifest))
			}
			for fileID, kr1 := range level.manifest {
				if kr2, ok := manifest[fileID]; !ok {
					t.Fatalf("Read manifest does not contain: %v while in-memory manifest does\n", fileID)
				} else {
					if kr1.startKey != kr2.startKey || kr1.endKey != kr2.endKey {
						t.Fatalf("Read manifest key range: %v does not match in-memory manifest key range: %v\n", kr2, kr1)
					}
				}
			}
		}
	}
}
