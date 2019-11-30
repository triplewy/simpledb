package db

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func insertIntoAVL(tree *avlTree, kvs []*kv) {
	for i, kv := range kvs {
		tree.Put(&lsmDataEntry{
			ts:        uint64(i),
			key:       kv.key,
			valueType: String,
			value:     []byte(kv.value.(string)),
		})
	}
}

func TestAVLPutLeftLeft(t *testing.T) {
	tree := newAVLTree()
	kvs := []*kv{
		&kv{key: "5", value: "5"},
		&kv{key: "4", value: "4"},
		&kv{key: "3", value: "3"},
	}
	insertIntoAVL(tree, kvs)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutLeftRight(t *testing.T) {
	tree := newAVLTree()
	kvs := []*kv{
		&kv{key: "5", value: "5"},
		&kv{key: "3", value: "3"},
		&kv{key: "4", value: "4"},
	}
	insertIntoAVL(tree, kvs)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightRight(t *testing.T) {
	tree := newAVLTree()
	kvs := []*kv{
		&kv{key: "3", value: "3"},
		&kv{key: "4", value: "4"},
		&kv{key: "5", value: "5"},
	}
	insertIntoAVL(tree, kvs)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightLeft(t *testing.T) {
	tree := newAVLTree()
	kvs := []*kv{
		&kv{key: "3", value: "3"},
		&kv{key: "5", value: "5"},
		&kv{key: "4", value: "4"},
	}
	insertIntoAVL(tree, kvs)
	result := strings.Join(tree.Preorder(), ",")
	if result != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", result)
	}
}

func TestAVLPutDuplicate(t *testing.T) {
	tree := newAVLTree()
	kvs := []*kv{
		&kv{key: "3", value: "3"},
		&kv{key: "5", value: "5"},
		&kv{key: "4", value: "4"},
		&kv{key: "3", value: "10"},
	}
	insertIntoAVL(tree, kvs)
	entries := tree.Inorder()
	values := []string{}
	for _, entry := range entries {
		values = append(values, string(entry.value))
	}
	result := strings.Join(values, ",")
	if result != "10,3,4,5" {
		t.Fatalf("Expected: 10,3,4,5 Got: %s\n", result)
	}

	kvs = []*kv{
		&kv{key: "5", value: "15"},
		&kv{key: "4", value: "40"},
	}
	insertIntoAVL(tree, kvs)

	entries = tree.Inorder()
	values = []string{}
	for _, entry := range entries {
		values = append(values, string(entry.value))
	}
	result = strings.Join(values, ",")
	if result != "10,3,40,4,15,5" {
		t.Fatalf("Expected: 10,3,40,4,15,5 Got: %s\n", result)
	}
}

func TestAVLScan(t *testing.T) {
	tree := newAVLTree()
	kvs := []*kv{
		&kv{key: "0", value: "0"},
		&kv{key: "2", value: "2"},
		&kv{key: "4", value: "4"},
		&kv{key: "5", value: "5"},
		&kv{key: "6", value: "6"},
		&kv{key: "8", value: "8"},
		&kv{key: "9", value: "9"},
		&kv{key: "9", value: "90"},
	}
	insertIntoAVL(tree, kvs)

	entries := tree.Scan(&keyRange{startKey: "4", endKey: "6"}, 100)
	result := []string{}
	for _, entry := range entries {
		result = append(result, entry.key)
	}
	if strings.Join(result, ",") != "4,5,6" {
		t.Fatalf("Expected: 4,5,6 Got: %s\n", strings.Join(result, ","))
	}
	entries = tree.Scan(&keyRange{startKey: "2", endKey: "6"}, 100)
	result = []string{}
	for _, entry := range entries {
		result = append(result, entry.key)
	}
	if strings.Join(result, ",") != "2,4,5,6" {
		t.Fatalf("Expected: 2,4,5,6 Got: %s\n", strings.Join(result, ","))
	}
	entries = tree.Scan(&keyRange{startKey: "11", endKey: "12"}, 100)
	result = []string{}
	for _, entry := range entries {
		result = append(result, entry.key)
	}
	if strings.Join(result, ",") != "" {
		t.Fatalf("Expected:  Got: %s\n", strings.Join(result, ","))
	}
	entries = tree.Scan(&keyRange{startKey: "0", endKey: "9"}, 100)
	result = []string{}
	for _, entry := range entries {
		result = append(result, string(entry.value))
	}
	if strings.Join(result, ",") != "0,2,4,5,6,8,90" {
		t.Fatalf("Expected: 0,2,4,5,6,8,90 Got: %s\n", strings.Join(result, ","))
	}
}

func TestAVLBulk(t *testing.T) {
	tree := newAVLTree()
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
	tree := newAVLTree()
	memorykv := make(map[string]string)

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(rand.Intn(100))
		value := strconv.Itoa(i)
		memorykv[key] = value
		entry, err := createDataEntry(uint64(i), key, value)
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		tree.Put(entry)
	}

	for key, value := range memorykv {
		entry := tree.Find(key, 2000)
		if entry == nil {
			t.Fatalf("Key should be in the AVL Tree")
		} else {
			kv, err := parseDataEntry(entry)
			if err != nil {
				t.Fatalf("Error parsing data entry: %v\n", err)
			}
			if kv.value.(string) != value {
				t.Fatalf("Expected: %v, Got: %v\n", value, kv.value.(string))
			}
		}
	}
}
