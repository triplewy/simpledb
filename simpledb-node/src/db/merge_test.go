package db

import (
	"strconv"
	"strings"
	"testing"
)

func TestMergeAbove1(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	keys1 := []string{}
	keys2 := []string{}
	keys3 := []string{}
	keys4 := []string{}

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		if 1000000000000000000+i%4 == 0 {
			keys1 = append(keys1, key)
		} else if 1000000000000000000+i-1%4 == 0 {
			keys2 = append(keys2, key)
		} else if 1000000000000000000+i-2%4 == 0 {
			keys3 = append(keys3, key)
		} else {
			keys4 = append(keys4, key)
		}
	}

	err = populateSSTFile(keys1, "data/L0/test1")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = populateSSTFile(keys2, "data/L0/test2")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = populateSSTFile(keys3, "data/L0/test3")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = populateSSTFile(keys4, "data/L0/test4")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}

	entries, err := mergeSort([]string{"data/L0/test2", "data/L0/test1", "data/L0/test3", "data/L0/test4"})
	if err != nil {
		t.Errorf("Error merge sorting files: %v\n", err)
	}

	i := 0
	for _, entry := range entries {
		key := entry.key
		if key != strconv.Itoa(1000000000000000000+i) {
			t.Errorf("Did not sort files properly. Expected: %s, Got: %s\n", strconv.Itoa(1000000000000000000+i), string(key))
		}
		i++
	}
}

func TestMergeAbove2(t *testing.T) {
	err := DeleteData()
	if err != nil {
		t.Errorf("Error deleting data: %v\n", err)
	}

	keys1 := []string{}
	keys2 := []string{}
	keys3 := []string{}

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(1000000000000000000 + i)
		if 1000000000000000000+i%4 == 0 {
			keys1 = append(keys1, key)
		} else if 1000000000000000000+i-1%4 == 0 {
			keys2 = append(keys2, key)
		} else {
			keys3 = append(keys3, key)
		}
	}

	err = populateSSTFile(keys1, "data/L0/test1")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = populateSSTFile(keys2, "data/L0/test2")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}
	err = populateSSTFile(keys3, "data/L0/test3")
	if err != nil {
		t.Errorf("Error populating file: %v\n", err)
	}

	entries, err := mergeSort([]string{"data/L0/test2", "data/L0/test1", "data/L0/test3"})
	if err != nil {
		t.Errorf("Error merge sorting files: %v\n", err)
	}

	i := 0
	for _, entry := range entries {
		key := entry.key
		if key != strconv.Itoa(1000000000000000000+i) {
			t.Errorf("Did not sort files properly. Expected: %s, Got: %s\n", strconv.Itoa(1000000000000000000+i), string(key))
		}
		i++
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
