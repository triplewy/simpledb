package db

import (
	"fmt"
	"strconv"
	"testing"
)

func TestBloom(t *testing.T) {
	n := 100000
	bloom := newBloom(n)

	for i := 0; i < n; i++ {
		key := strconv.Itoa(i)
		bloom.Insert(key)
	}

	falsePositives := 0

	for i := 0; i < n*2; i++ {
		key := strconv.Itoa(i)
		result := bloom.Check(key)
		if i < n {
			if !result {
				t.Fatalf("Encountered False Negative!")
			}
		} else {
			if result {
				falsePositives++
			}
		}
	}

	denom := float64(n * 2)
	fmt.Printf("Total Correct: %f%%, False Positives: %f%%\n", float64(n*2-falsePositives)/denom*float64(100), float64(falsePositives)/denom*float64(100))
}
