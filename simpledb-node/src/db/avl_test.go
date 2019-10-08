package db

import (
	"strings"
	"testing"
)

func TestAVLPutLeftLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("5", "5")
	tree.Put("4", "4")
	tree.Put("3", "3")
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutLeftRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("5", "5")
	tree.Put("3", "3")
	tree.Put("4", "4")
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightRight(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3")
	tree.Put("4", "4")
	tree.Put("5", "5")
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutRightLeft(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3")
	tree.Put("5", "5")
	tree.Put("4", "4")
	preorder := strings.Join(tree.Preorder(), ",")
	if preorder != "4,3,5" {
		t.Fatalf("Expected: 4,3,5 Got: %s\n", preorder)
	}
}

func TestAVLPutError(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3")
	err := tree.Put("3", "3")
	if err == nil || err.Error() != "Key already exists" {
		t.Fatalf("Expected: Key already exists, Got: %s\n", err)
	}
}
