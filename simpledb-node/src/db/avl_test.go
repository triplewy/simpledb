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

func TestAVLPutDuplicate(t *testing.T) {
	tree := NewAVLTree()
	tree.Put("3", "3")
	tree.Put("5", "5")
	tree.Put("4", "4")
	tree.Put("3", "10")

	preorder := strings.Join(tree.PreorderValues(), ",")
	if preorder != "4,10,5" {
		t.Fatalf("Expected: 4,10,5 Got: %s\n", preorder)
	}

	tree.Put("5", "15")
	tree.Put("4", "40")

	preorder = strings.Join(tree.PreorderValues(), ",")
	if preorder != "40,10,15" {
		t.Fatalf("Expected: 40,10,15 Got: %s\n", preorder)
	}
}
