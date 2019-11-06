package db

import (
	"math/rand"
	"strconv"
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

func TestAVLBulk(t *testing.T) {
	tree := NewAVLTree()
	for i := 1000; i < 5000; i++ {
		entry, err := createDataEntry(uint64(i), strconv.Itoa(i), int64(i))
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		tree.Put(entry)
	}

	entries := tree.Inorder()
	for i, entry := range entries {
		kv, err := parseDataEntry(entry)
		if err != nil {
			t.Fatalf("Error parsing data entry: %v\n", err)
		}
		if kv.key != strconv.Itoa(i+1000) {
			t.Fatalf("Expected Key: %v, Got %v\n", strconv.Itoa(i), kv.key)
		}
		if kv.value.(int64) != int64(i+1000) {
			t.Fatalf("Expected Value: %d, Got %d\n", i, kv.value.(int64))
		}
	}
}

func TestAVLRandom(t *testing.T) {
	tree := NewAVLTree()
	memoryKV := make(map[string]string)

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(rand.Intn(100))
		value := strconv.Itoa(i)
		memoryKV[key] = value
		entry, err := createDataEntry(uint64(i), key, value)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		tree.Put(entry)
	}

	for key, value := range memoryKV {
		node := tree.Find(key)
		if node == nil {
			t.Fatalf("Key should be in the AVL Tree")
		} else {
			kv, err := parseDataEntry(node.entry)
			if err != nil {
				t.Fatalf("Error parsing data entry: %v\n", err)
			}
			if kv.value.(string) != value {
				t.Fatalf("Expected: %v, Got: %v\n", value, kv.value.(string))
			}
		}
	}
}
