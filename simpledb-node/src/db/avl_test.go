package db

import (
	"strings"
	"testing"
)

func TestAVLPutLeftLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Put(&LSMDataEntry{key: "5"})
	tree.Put(&LSMDataEntry{key: "4"})
	tree.Put(&LSMDataEntry{key: "3"})
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutLeftRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Put(&LSMDataEntry{key: "5"})
	tree.Put(&LSMDataEntry{key: "3"})
	tree.Put(&LSMDataEntry{key: "4"})
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Put(&LSMDataEntry{key: "3"})
	tree.Put(&LSMDataEntry{key: "4"})
	tree.Put(&LSMDataEntry{key: "5"})
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Put(&LSMDataEntry{key: "3"})
	tree.Put(&LSMDataEntry{key: "5"})
	tree.Put(&LSMDataEntry{key: "4"})
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutDuplicate(t *testing.T) {
	tree := NewAVLTree()
	tree.Put(&LSMDataEntry{key: "3", valueType: String, value: []byte("3")})
	tree.Put(&LSMDataEntry{key: "5", valueType: String, value: []byte("5")})
	tree.Put(&LSMDataEntry{key: "4", valueType: String, value: []byte("4")})
	tree.Put(&LSMDataEntry{key: "3", valueType: String, value: []byte("10")})

	entries := tree.PreorderValues()
	values := make([]string, len(entries))
	for i, entry := range entries {
		kv, err := parseDataEntry(entry)
		if err != nil {
			t.Fatalf("Error parsing data entry: %v\n", err)
		}
		values[i] = kv.value.(string)
	}
	preorder := strings.Join(values, ",")
	if preorder != "4,10,5" {
		t.Fatalf("Expected: 4,10,5 Got: %s\n", preorder)
	}

	tree.Put(&LSMDataEntry{key: "5", valueType: String, value: []byte("15")})
	tree.Put(&LSMDataEntry{key: "4", valueType: String, value: []byte("40")})

	entries = tree.PreorderValues()
	values = make([]string, len(entries))
	for i, entry := range entries {
		kv, err := parseDataEntry(entry)
		if err != nil {
			t.Fatalf("Error parsing data entry: %v\n", err)
		}
		values[i] = kv.value.(string)
	}
	preorder = strings.Join(values, ",")
	if preorder != "40,10,15" {
		t.Fatalf("Expected: 40,10,15 Got: %s\n", preorder)
	}
}

func TestAVLRange(t *testing.T) {
	tree := NewAVLTree()
	tree.Put(&LSMDataEntry{key: "0"})
	tree.Put(&LSMDataEntry{key: "2"})
	tree.Put(&LSMDataEntry{key: "4"})
	tree.Put(&LSMDataEntry{key: "5"})
	tree.Put(&LSMDataEntry{key: "6"})
	tree.Put(&LSMDataEntry{key: "8"})
	tree.Put(&LSMDataEntry{key: "9"})

	pairs := tree.Range("4", "6")
	result := []string{}

	for _, pair := range pairs {
		result = append(result, pair.key)
	}

	if strings.Join(result, ",") != "4,5,6" {
		t.Fatalf("Expected: 4,5,6 Got: %s\n", strings.Join(result, ","))
	}

	pairs = tree.Range("2", "6")
	result = []string{}

	for _, pair := range pairs {
		result = append(result, pair.key)
	}

	if strings.Join(result, ",") != "2,4,5,6" {
		t.Fatalf("Expected: 2,4,5,6 Got: %s\n", strings.Join(result, ","))
	}

	pairs = tree.Range("11", "12")
	result = []string{}

	for _, pair := range pairs {
		result = append(result, pair.key)
	}

	if strings.Join(result, ",") != "" {
		t.Fatalf("Expected:  Got: %s\n", strings.Join(result, ","))
	}

	pairs = tree.Range("0", "9")
	result = []string{}

	for _, pair := range pairs {
		result = append(result, pair.key)
	}

	if strings.Join(result, ",") != "0,2,4,5,6,8,9" {
		t.Fatalf("Expected: 0,2,4,5,6,8,9 Got: %s\n", strings.Join(result, ","))
	}
}
