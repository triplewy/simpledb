package db

import (
	"os"
	"sort"
)

func mergeSort(files []string) ([][]byte, error) {
	if len(files) > 1 {
		mid := len(files) / 2

		left, err := mergeSort(files[:mid])
		if err != nil {
			return nil, err
		}
		right, err := mergeSort(files[mid:])
		if err != nil {
			return nil, err
		}

		result := mergeHelper(left, right)

		return result, nil
	}

	return mmap(files[0])
}

func mergeHelper(left, right [][]byte) [][]byte {
	od := newOrderedDict()
	i, j := 0, 0

	for i < len(left) && j < len(right) {
		leftEntry := newODValue(left[i])
		rightEntry := newODValue(right[j])

		if val, ok := od.Get(leftEntry.Key()); ok {
			if val.(odValue).Offset() < leftEntry.Offset() {
				od.Set(leftEntry.Key(), leftEntry)
			}
			i++
			continue
		}

		if val, ok := od.Get(rightEntry.Key()); ok {
			if val.(odValue).Offset() < rightEntry.Offset() {
				od.Set(rightEntry.Key(), rightEntry)
			}
			j++
			continue
		}

		if leftEntry.Key() < rightEntry.Key() {
			od.Set(leftEntry.Key(), leftEntry)
			i++
		} else if leftEntry.Key() == rightEntry.Key() {
			if leftEntry.Offset() > rightEntry.Offset() {
				od.Set(leftEntry.Key(), leftEntry)
			} else {
				od.Set(rightEntry.Key(), rightEntry)
			}
			i++
			j++
		} else {
			od.Set(rightEntry.Key(), rightEntry)
			j++
		}
	}

	for i < len(left) {
		leftEntry := newODValue(left[i])
		od.Set(leftEntry.Key(), leftEntry)
		i++
	}

	for j < len(right) {
		rightEntry := newODValue(right[j])
		od.Set(rightEntry.Key(), rightEntry)
		j++
	}

	result := [][]byte{}

	for val := range od.Iterate() {
		result = append(result, val.(odValue).Entry())
	}

	return result
}

func mmap(filename string) ([][]byte, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	dataSize, _, _, _, err := readHeader(f)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, dataSize)

	numBytes, err := f.ReadAt(buffer, headerSize)
	if err != nil {
		return nil, err
	}

	if numBytes != len(buffer) {
		return nil, newErrReadUnexpectedBytes("Data Block")
	}

	keys := [][]byte{}

	for i := 0; i < len(buffer); i += blockSize {
		block := buffer[i : i+blockSize]
		j := 0
		for j < len(block) && block[j] != byte(0) {
			keySize := uint8(block[j])
			j++
			entry := make([]byte, 13+int(keySize))
			copy(entry[0:], []byte{keySize})
			copy(entry[1:], block[j:j+int(keySize)+12])
			j += int(keySize) + 12
			keys = append(keys, entry)
		}
	}

	return keys, nil
}

func mergeIntervals(intervals []*merge) []*merge {
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].keyRange.startKey < intervals[j].keyRange.startKey
	})

	result := []*merge{}

	for i, interval := range intervals {
		if i == 0 {
			result = append(result, interval)
		} else {
			curr := result[len(result)-1]
			currEnd := curr.keyRange.endKey
			nextStart := interval.keyRange.startKey
			nextEnd := interval.keyRange.endKey

			if nextStart > currEnd {
				result = append(result, interval)
			} else {
				if nextEnd > currEnd {
					curr.keyRange.endKey = nextEnd
					curr.files = append(curr.files, interval.files...)
				}
				curr.files = append(curr.files, interval.files...)
			}
		}
	}

	return result
}

// mergeInterval joins the interval into the list if it overlaps with an interval. Else it returns the original list.
func mergeInterval(intervals []*merge, newInterval *merge) (merged bool, result []*merge) {
	merged = false
	newLeft := newInterval.keyRange.startKey
	newRight := newInterval.keyRange.endKey
	newFiles := newInterval.files

	for i, interval := range intervals {
		left := interval.keyRange.startKey
		right := interval.keyRange.endKey

		if right < newLeft {
			result = append(result, interval)
		} else if (left <= newLeft && newLeft <= right) || (left <= newRight && newRight <= right) || (newLeft <= left && newRight >= right) {
			if left < newLeft {
				newLeft = left
			}
			if right > newRight {
				newRight = right
			}
			newFiles = append(newFiles, interval.files...)
			merged = true
		} else {
			if merged {
				result = append(result, &merge{
					files: newFiles,
					keyRange: &KeyRange{
						startKey: newLeft,
						endKey:   newRight,
					},
				})
				return true, append(result, intervals[i:]...)
			}
			return false, intervals
		}
	}

	if merged {
		return true, append(result, &merge{
			files: newFiles,
			keyRange: &KeyRange{
				startKey: newLeft,
				endKey:   newRight,
			},
		})
	}
	return false, intervals
}
