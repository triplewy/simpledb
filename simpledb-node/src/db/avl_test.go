package db

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func insertIntoAVL(tree *avlTree, entries []*Entry) {
	for i, entry := range entries {
		entry.ts = uint64(i)
		tree.Put(entry)
	}
}

func TestAVLPutLeftLeft(t *testing.T) {
	tree := newAVLTree()
	entries := []*Entry{
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("5")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("4")}}},
		&Entry{Key: "3", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("3")}}},
	}
	insertIntoAVL(tree, entries)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutLeftRight(t *testing.T) {
	tree := newAVLTree()
	entries := []*Entry{
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("5")}}},
		&Entry{Key: "3", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("3")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("4")}}},
	}
	insertIntoAVL(tree, entries)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightRight(t *testing.T) {
	tree := newAVLTree()
	entries := []*Entry{
		&Entry{Key: "3", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("3")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("4")}}},
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("5")}}},
	}
	insertIntoAVL(tree, entries)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightLeft(t *testing.T) {
	tree := newAVLTree()
	entries := []*Entry{
		&Entry{Key: "3", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("3")}}},
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("5")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("4")}}},
	}
	insertIntoAVL(tree, entries)
	result := strings.Join(tree.Preorder(), ",")
	if result != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", result)
	}
}

func TestAVLPutDuplicate(t *testing.T) {
	tree := newAVLTree()
	entries := []*Entry{
		&Entry{Key: "3", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("3")}}},
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("5")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("4")}}},
		&Entry{Key: "3", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("10")}}},
	}
	insertIntoAVL(tree, entries)
	entries = tree.Inorder()
	values := []string{}
	for _, entry := range entries {
		values = append(values, string(entry.Fields["value"].Data))
	}
	result := strings.Join(values, ",")
	if result != "10,3,4,5" {
		t.Fatalf("Expected: 10,3,4,5 Got: %s\n", result)
	}

	entries = []*Entry{
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("15")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("40")}}},
	}
	insertIntoAVL(tree, entries)
	entries = tree.Inorder()
	values = []string{}
	for _, entry := range entries {
		values = append(values, string(entry.Fields["value"].Data))
	}
	result = strings.Join(values, ",")
	if result != "10,3,40,4,15,5" {
		t.Fatalf("Expected: 10,3,40,4,15,5 Got: %s\n", result)
	}
}

func TestAVLScan(t *testing.T) {
	tree := newAVLTree()
	entries := []*Entry{
		&Entry{Key: "0", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("0")}}},
		&Entry{Key: "2", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("2")}}},
		&Entry{Key: "4", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("4")}}},
		&Entry{Key: "5", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("5")}}},
		&Entry{Key: "6", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("6")}}},
		&Entry{Key: "8", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("8")}}},
		&Entry{Key: "9", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("9")}}},
		&Entry{Key: "9", Fields: map[string]*Value{"value": &Value{DataType: String, Data: []byte("90")}}},
	}
	insertIntoAVL(tree, entries)
	entries = tree.Scan(&keyRange{startKey: "4", endKey: "6"}, 100)
	result := []string{}
	for _, entry := range entries {
		result = append(result, entry.Key)
	}
	if strings.Join(result, ",") != "4,5,6" {
		t.Fatalf("Expected: 4,5,6 Got: %s\n", strings.Join(result, ","))
	}
	entries = tree.Scan(&keyRange{startKey: "2", endKey: "6"}, 100)
	result = []string{}
	for _, entry := range entries {
		result = append(result, entry.Key)
	}
	if strings.Join(result, ",") != "2,4,5,6" {
		t.Fatalf("Expected: 2,4,5,6 Got: %s\n", strings.Join(result, ","))
	}
	entries = tree.Scan(&keyRange{startKey: "11", endKey: "12"}, 100)
	result = []string{}
	for _, entry := range entries {
		result = append(result, entry.Key)
	}
	if strings.Join(result, ",") != "" {
		t.Fatalf("Expected:  Got: %s\n", strings.Join(result, ","))
	}
	entries = tree.Scan(&keyRange{startKey: "0", endKey: "9"}, 100)
	result = []string{}
	for _, entry := range entries {
		result = append(result, string(entry.Fields["value"].Data))
	}
	if strings.Join(result, ",") != "0,2,4,5,6,8,90" {
		t.Fatalf("Expected: 0,2,4,5,6,8,90 Got: %s\n", strings.Join(result, ","))
	}
}

func TestAVLBulk(t *testing.T) {
	tree := newAVLTree()
	for i := 1000; i < 5000; i++ {
		entry, err := createEntry(uint64(i), strconv.Itoa(i), map[string]interface{}{"value": strconv.Itoa(i)})
		if err != nil {
			t.Fatalf("Error creating data entry: %v\n", err)
		}
		tree.Put(entry)
	}

	entries := tree.Inorder()
	for i, entry := range entries {
		if entry.Key != strconv.Itoa(i+1000) {
			t.Fatalf("Expected Key: %v, Got %v\n", strconv.Itoa(i), entry.Key)
		}
		if string(entry.Fields["value"].Data) != strconv.Itoa(i+1000) {
			t.Fatalf("Expected Value: %v, Got %v\n", strconv.Itoa(i+1000), string(entry.Fields["value"].Data))
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
		entry, err := createEntry(uint64(i), key, map[string]interface{}{"value": value})
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
			if string(entry.Fields["value"].Data) != value {
				t.Fatalf("Expected: %v, Got: %v\n", value, string(entry.Fields["value"].Data))
			}
		}
	}
}
