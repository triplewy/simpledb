package db

import (
	"strings"
	"testing"
)

func TestLRU(t *testing.T) {
	lru := newLRU(5)
	keyTs := []*Entry{
		&Entry{key: "1", value: uint64(1)},
		&Entry{key: "2", value: uint64(2)},
		&Entry{key: "3", value: uint64(3)},
		&Entry{key: "4", value: uint64(4)},
		&Entry{key: "5", value: uint64(5)},
		&Entry{key: "5", value: uint64(6)},
		&Entry{key: "2", value: uint64(7)},
		&Entry{key: "6", value: uint64(8)},
	}

	for _, kv := range keyTs {
		lru.Insert(kv.key, kv.value.(uint64))
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
