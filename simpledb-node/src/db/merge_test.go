package db

import (
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMergeMMap(t *testing.T) {
	err := deleteData("data")
	if err != nil {
		t.Fatalf("Error deleting data: %v\n", err)
	}

	memorykv := make(map[string]string)
	entries := []*Entry{}

	for i := 10000; i < 50000; i++ {
		key := strconv.Itoa(i)
		value := uuid.New().String()
		entries = append(entries, simpleEntry(uint64(i), key, value))
		memorykv[key] = value
	}

	dataBlocks, indexBlock, bloom, keyRange, err := writeEntries(entries)
	if err != nil {
		t.Fatalf("Error writing data entries: %v\n", err)
	}

	keyRangeEntry := createkeyRangeEntry(keyRange)
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
		if entry.Key != key || string(entry.Fields["value"].Data) != memorykv[key] {
			t.Fatalf("Key or value expected: %v, Got key: %v value: %v\n", key, entry.Key, string(entry.Fields["value"].Data))
		}
	}
}

func TestMergeIntervals(t *testing.T) {
	intervals := []*merge{
		&merge{
			files: []string{"a"},
			keyRange: &keyRange{
				startKey: "a",
				endKey:   "c",
			},
		},
		&merge{
			files: []string{"b"},
			keyRange: &keyRange{
				startKey: "b",
				endKey:   "f",
			},
		},
		&merge{
			files: []string{"c"},
			keyRange: &keyRange{
				startKey: "h",
				endKey:   "j",
			},
		},
		&merge{
			files: []string{"d"},
			keyRange: &keyRange{
				startKey: "o",
				endKey:   "r",
			},
		},
	}

	intervals = mergeIntervals(intervals)

	expected := []*merge{
		&merge{
			files: []string{"a", "b"},
			keyRange: &keyRange{
				startKey: "a",
				endKey:   "f",
			},
		},
		&merge{
			files: []string{"c"},
			keyRange: &keyRange{
				startKey: "h",
				endKey:   "j",
			},
		},
		&merge{
			files: []string{"d"},
			keyRange: &keyRange{
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
			keyRange: &keyRange{
				startKey: "a",
				endKey:   "c",
			},
		},
	}

	interval := &merge{
		files: []string{"b"},
		keyRange: &keyRange{
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
		t.Fatalf("Interval, Expected: %v, Got: %v\n", &keyRange{startKey: "a", endKey: "f"}, intervals[0].keyRange)
	}

	interval = &merge{
		files: []string{"c"},
		keyRange: &keyRange{
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
		t.Fatalf("Interval, Expected: %v, Got: %v\n", &keyRange{startKey: "a", endKey: "f"}, intervals[0].keyRange)
	}
}
