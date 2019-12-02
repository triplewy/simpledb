package db

import (
	"strings"
	"testing"
)

func TestLRU(t *testing.T) {
	lru := newLRU(5)
	keys := []string{"1", "2", "3", "4", "5", "5", "2", "6"}
	values := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	for i := range keys {
		lru.Insert(keys[i], values[i])
	}

	result := []string{}
	curr := lru.list.head.next
	for curr != lru.list.tail {
		result = append(result, curr.key)
		curr = curr.next
	}

	if strings.Join(result, ",") != "3,4,5,2,6" {
		t.Fatalf("lru expected: %v, got %v\n", "3,4,5,2,6", strings.Join(result, ","))
	}

	if lru.maxTs != 1 {
		t.Fatalf("lru expected max Ts: 1, Got: %v\n", lru.maxTs)
	}

	ts, ok := lru.Get("2")
	if !ok {
		t.Fatalf("2 should exist in LRU")
	}
	if ts != 7 {
		t.Fatalf("lru expected ts: 7, Got: %v\n", ts)
	}
}
