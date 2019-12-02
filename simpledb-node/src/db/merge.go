package db

import (
	"sort"
)

func (level *level) mergeSort(files []string) ([]*Entry, error) {
	if len(files) > 1 {
		mid := len(files) / 2
		left, err := level.mergeSort(files[:mid])
		if err != nil {
			return nil, err
		}
		right, err := level.mergeSort(files[mid:])
		if err != nil {
			return nil, err
		}
		result := mergeHelper(left, right)
		return result, nil
	}
	return level.fm.MMap(files[0])
}

func mergeHelper(left, right []*Entry) (entries []*Entry) {
	entries = append(left, right...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key == entries[j].Key {
			return entries[i].ts > entries[j].ts
		}
		return entries[i].Key < entries[j].Key
	})
	return entries
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
					keyRange: &keyRange{
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
			keyRange: &keyRange{
				startKey: newLeft,
				endKey:   newRight,
			},
		})
	}
	return false, intervals
}
