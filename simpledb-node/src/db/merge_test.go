package db

import (
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMerge(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	entries1 := []*LSMDataEntry{}
	entries2 := []*LSMDataEntry{}
	entries3 := []*LSMDataEntry{}
	entries4 := []*LSMDataEntry{}

	for i := 0; i < 50000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		entry, err := createDataEntry(uint64(i), key, key)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}

		if (1000000000000000000+i)%4 == 0 {
			entries1 = append(entries1, entry)
		} else if (1000000000000000000+i-1)%4 == 0 {
			entries2 = append(entries2, entry)
		} else if (1000000000000000000+i-2)%4 == 0 {
			entries3 = append(entries3, entry)
		} else {
			entries4 = append(entries4, entry)
		}
	}

	err = writeEntriesToFile("data/L0/test1", entries1)
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = writeEntriesToFile("data/L0/test2", entries2)
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = writeEntriesToFile("data/L0/test3", entries3)
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = writeEntriesToFile("data/L0/test4", entries4)
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}

	entries, err := mergeSort([]string{"data/L0/test2", "data/L0/test1", "data/L0/test3", "data/L0/test4"})
	if err != nil {
		t.Errorf("Error merge sorting files: %v\n", err)
	}

	if len(entries) != 50000 {
		t.Fatalf("Length of entries expected: %d, Got: %d\n", 50000, len(entries))
	}

	for i, entry := range entries {
		key := entry.key
		if key != strconv.Itoa(1000000000000000000+i) {
			t.Errorf("Did not sort files properly. Expected: %s, Got: %s\n", strconv.Itoa(1000000000000000000+i), string(key))
		}
	}
}

func TestMergeMMap(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	memoryKV := make(map[string]string)
	entries := []*LSMDataEntry{}

	for i := 10000; i < 50000; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		memoryKV[key] = value
		entry, err := createDataEntry(uint64(i), key, value)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		entries = append(entries, entry)
	}

	dataBlocks, indexBlock, bloom, keyRange, err := writeDataEntries(entries)
	if err != nil {
		t.Fatalf("Error writing data entries: %v\n", err)
	}

	keyRangeEntry := createKeyRangeEntry(keyRange)
	header := createHeader(len(dataBlocks), len(indexBlock), len(bloom.bits), len(keyRangeEntry))
	data := append(header, append(append(append(dataBlocks, indexBlock...), bloom.bits...), keyRangeEntry...)...)

	err = writeNewFile("data/L0/test.sst", data)
	if err != nil {
		t.Fatalf("Error writing to file: %v\n", err)
	}

	entries, err = mmap("data/L0/test.sst")
	if err != nil {
		t.Fatalf("Error mmaping file: %v\n", err)
	}
	if len(entries) != 40000 {
		t.Fatalf("Expected length of entries: %d, Got %d\n", 40000, len(entries))
	}

	for i, entry := range entries {
		key := strconv.Itoa(i + 10000)
		kv, err := parseDataEntry(entry)
		if err != nil {
			t.Fatalf("Error parsing data entry: %v\n", err)
		}
		if kv.key != key || kv.value.(string) != memoryKV[key] {
			t.Fatalf("Key or value expected: %v, Got key: %v value: %v\n", key, kv.key, kv.value.(string))
		}
	}
}

func TestMergeIntervals(t *testing.T) {
	intervals := []*merge{
		&merge{
			files: []string{"a"},
			keyRange: &KeyRange{
				startKey: "a",
				endKey:   "c",
			},
		},
		&merge{
			files: []string{"b"},
			keyRange: &KeyRange{
				startKey: "b",
				endKey:   "f",
			},
		},
		&merge{
			files: []string{"c"},
			keyRange: &KeyRange{
				startKey: "h",
				endKey:   "j",
			},
		},
		&merge{
			files: []string{"d"},
			keyRange: &KeyRange{
				startKey: "o",
				endKey:   "r",
			},
		},
	}

	intervals = mergeIntervals(intervals)

	expected := []*merge{
		&merge{
			files: []string{"a", "b"},
			keyRange: &KeyRange{
				startKey: "a",
				endKey:   "f",
			},
		},
		&merge{
			files: []string{"c"},
			keyRange: &KeyRange{
				startKey: "h",
				endKey:   "j",
			},
		},
		&merge{
			files: []string{"d"},
			keyRange: &KeyRange{
				startKey: "o",
				endKey:   "r",
			},
		},
	}

	if len(expected) != len(intervals) {
		t.Fatalf("Expected intervals to have length: %d, got %d\n", len(expected), len(intervals))
	}

	for i := 0; i < len(expected); i++ {
		interval := intervals[i]
		expect := expected[i]
		if strings.Join(interval.files, ",") != strings.Join(expect.files, ",") {
			t.Fatalf("Files, Expected: %v, Got: %v\n", strings.Join(expect.files, ","), strings.Join(interval.files, ","))
		}
		if interval.keyRange.startKey != expect.keyRange.startKey || interval.keyRange.endKey != expect.keyRange.endKey {
			t.Fatalf("Interval, Expected: %v, Got: %v\n", expect.keyRange, interval.keyRange)
		}
	}
}

func TestMergeInterval(t *testing.T) {
	intervals := []*merge{
		&merge{
			files: []string{"a"},
			keyRange: &KeyRange{
				startKey: "a",
				endKey:   "c",
			},
		},
	}

	interval := &merge{
		files: []string{"b"},
		keyRange: &KeyRange{
			startKey: "b",
			endKey:   "f",
		},
	}

	merged, intervals := mergeInterval(intervals, interval)
	if !merged {
		t.Fatalf("Expected interval to be merged\n")
	}
	if strings.Join(intervals[0].files, ",") != "a,b" && strings.Join(intervals[0].files, ",") != "b,a" {
		t.Fatalf("Files, Expected: %v, Got: %v\n", "a,b", strings.Join(intervals[0].files, ","))
	}
	if intervals[0].keyRange.startKey != "a" || intervals[0].keyRange.endKey != "f" {
		t.Fatalf("Interval, Expected: %v, Got: %v\n", &KeyRange{startKey: "a", endKey: "f"}, intervals[0].keyRange)
	}

	interval = &merge{
		files: []string{"c"},
		keyRange: &KeyRange{
			startKey: "h",
			endKey:   "j",
		},
	}

	merged, intervals = mergeInterval(intervals, interval)
	if merged {
		t.Fatalf("Expected interval to not be merged\n")
	}
	if len(intervals) > 1 {
		t.Fatalf("Len(intervals), Expected: 1, Got: %d\n", len(intervals))
	}
	if strings.Join(intervals[0].files, ",") != "a,b" && strings.Join(intervals[0].files, ",") != "b,a" {
		t.Fatalf("Files, Expected: %v, Got: %v\n", "a,b", strings.Join(intervals[0].files, ","))
	}
	if intervals[0].keyRange.startKey != "a" || intervals[0].keyRange.endKey != "f" {
		t.Fatalf("Interval, Expected: %v, Got: %v\n", &KeyRange{startKey: "a", endKey: "f"}, intervals[0].keyRange)
	}
}
