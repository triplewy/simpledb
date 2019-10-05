package simpledb

import (
	"strings"
	"testing"
)

func TestAVLInsertLeftLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("5"), 0, 0)
	tree.Insert([]byte("4"), 0, 0)
	tree.Insert([]byte("3"), 0, 0)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLInsertLeftRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("5"), 0, 0)
	tree.Insert([]byte("3"), 0, 0)
	tree.Insert([]byte("4"), 0, 0)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLInsertRightRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("3"), 0, 0)
	tree.Insert([]byte("4"), 0, 0)
	tree.Insert([]byte("5"), 0, 0)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLInsertRightLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("3"), 0, 0)
	tree.Insert([]byte("5"), 0, 0)
	tree.Insert([]byte("4"), 0, 0)
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLInsertError(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("3"), 0, 0)
	err := tree.Insert([]byte("3"), 0, 0)
	if err == nil || err.Error() != "Key already exists" {
		t.Fatalf("Expected: Key already exists, Got: %s\n", err)
	}
}
