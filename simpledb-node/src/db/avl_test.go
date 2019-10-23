package db

import (
	"strings"
	"testing"
)

func TestAVLPutLeftLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("5", "5", nil)
	tree.Put("4", "4", nil)
	tree.Put("3", "3", nil)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutLeftRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("5", "5", nil)
	tree.Put("3", "3", nil)
	tree.Put("4", "4", nil)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3", nil)
	tree.Put("4", "4", nil)
	tree.Put("5", "5", nil)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3", nil)
	tree.Put("5", "5", nil)
	tree.Put("4", "4", nil)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutDuplicate(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3", nil)
	tree.Put("5", "5", nil)
	tree.Put("4", "4", nil)
	tree.Put("3", "10", nil)

	preorder := strings.Join(tree.PreorderValues(), ",")
	if preorder != "4,10,5" {
		t.Fatalf("Expected: 4,10,5 Got: %s\n", preorder)
	}

	tree.Put("5", "15", nil)
	tree.Put("4", "40", nil)

	preorder = strings.Join(tree.PreorderValues(), ",")
	if preorder != "40,10,15" {
		t.Fatalf("Expected: 40,10,15 Got: %s\n", preorder)
	}
}

func TestAVLRange(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("0", "0", nil)
	tree.Put("2", "2", nil)
	tree.Put("4", "4", nil)
	tree.Put("5", "5", nil)
	tree.Put("6", "6", nil)
	tree.Put("8", "8", nil)
	tree.Put("9", "9", nil)

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
