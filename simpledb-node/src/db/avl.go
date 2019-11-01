package db

import (
	"sync"
)

// AVLNode is node struct for AVL-Tree
type AVLNode struct {
	key   string
	entry *LSMDataEntry

	left   *AVLNode
	right  *AVLNode
	height int
}

// AVLTree is struct for AVL-Tree
type AVLTree struct {
	root *AVLNode
	sync.RWMutex
}

func newAVLNode(entry *LSMDataEntry) *AVLNode {
	return &AVLNode{
		key:   entry.key,
		entry: entry,

		left:   nil,
		right:  nil,
		height: 1,
	}
}

// NewAVLTree creates a new AVL-Tree
func NewAVLTree() *AVLTree {
	return &AVLTree{
		root: nil,
	}
}

// Put inserts a new node into an AVL-Tree
func (tree *AVLTree) Put(entry *LSMDataEntry) {
	tree.Lock()
	defer tree.Unlock()

	newNode := newAVLNode(entry)
	tree.root = put(tree.root, newNode)
}

// Find finds the node in an AVL-Tree given a key. Returns error if key not found
func (tree *AVLTree) Find(key string) *AVLNode {
	tree.RLock()
	defer tree.RUnlock()

	return find(tree.root, key)
}

// Range finds all nodes whose keys fall within the range query
func (tree *AVLTree) Range(startKey, endKey string) []*LSMDataEntry {
	tree.RLock()
	defer tree.RUnlock()

	node := commonParent(tree.root, startKey, endKey)
	if node == nil {
		return []*LSMDataEntry{}
	}
	return rangeQuery(node, startKey, endKey)
}

// Inorder prints inorder traversal of AVL-Tree
func (tree *AVLTree) Inorder() []*LSMDataEntry {
	tree.RLock()
	defer tree.RUnlock()

	return inorder(tree.root)
}

//Preorder prints keys of preorder traversal of AVL-Tree
func (tree *AVLTree) Preorder() []string {
	tree.RLock()
	defer tree.RUnlock()

	pairs := preorder(tree.root)
	result := make([]string, len(pairs))

	for i, pair := range pairs {
		result[i] = pair.key
	}

	return result
}

//PreorderValues prints values of preorder traversal of AVL-Tree
func (tree *AVLTree) PreorderValues() []*LSMDataEntry {
	tree.RLock()
	defer tree.RUnlock()

	return preorder(tree.root)
}

func put(root, newNode *AVLNode) *AVLNode {
	if root == nil {
		return newNode
	} else if newNode.key == root.key {
		root.entry = newNode.entry
		return root
	} else if newNode.key < root.key {
		root.left = put(root.left, newNode)
	} else {
		root.right = put(root.right, newNode)
	}

	root.height = 1 + max(getHeight(root.left), getHeight(root.right))
	balance := getBalance(root)

	// Case 1 - Left Left
	if balance > 1 && newNode.key < root.left.key {
		return rightRotate(root)
	}
	// Case 2 - Right Right
	if balance < -1 && newNode.key > root.right.key {
		return leftRotate(root)
	}
	// Case 3 - Left Right
	if balance > 1 && newNode.key > root.left.key {
		root.left = leftRotate(root.left)
		return rightRotate(root)
	}
	// Case 4 - Right Left
	if balance < -1 && newNode.key < root.right.key {
		root.right = rightRotate(root.right)
		return leftRotate(root)
	}

	return root
}

func getHeight(root *AVLNode) int {
	if root == nil {
		return 0
	}
	return root.height
}

func getBalance(root *AVLNode) int {
	if root == nil {
		return 0
	}
	return getHeight(root.left) - getHeight(root.right)
}

func leftRotate(z *AVLNode) *AVLNode {
	y := z.right
	T2 := y.left

	y.left = z
	z.right = T2

	z.height = 1 + max(getHeight(z.left), getHeight(z.right))
	y.height = 1 + max(getHeight(y.left), getHeight(y.right))

	return y
}

func rightRotate(z *AVLNode) *AVLNode {
	y := z.left
	T2 := y.right

	y.right = z
	z.left = T2

	z.height = 1 + max(getHeight(z.left), getHeight(z.right))
	y.height = 1 + max(getHeight(y.left), getHeight(y.right))

	return y
}

func find(root *AVLNode, key string) *AVLNode {
	if root == nil {
		return nil
	}
	if root.key == key {
		return root
	}
	if key < root.key {
		return find(root.left, key)
	}
	return find(root.right, key)
}

func commonParent(root *AVLNode, startKey, endKey string) *AVLNode {
	if root == nil {
		return nil
	}
	if startKey < root.key && endKey < root.key {
		return commonParent(root.left, startKey, endKey)
	}
	if startKey > root.key && endKey > root.key {
		return commonParent(root.right, startKey, endKey)
	}
	return root
}

func rangeQuery(root *AVLNode, startKey, endKey string) []*LSMDataEntry {
	if root == nil {
		return []*LSMDataEntry{}
	}

	if startKey <= root.key && root.key <= endKey {
		leftKeys := rangeQuery(root.left, startKey, endKey)
		rightKeys := rangeQuery(root.right, startKey, endKey)

		result := append(leftKeys, root.entry)
		result = append(result, rightKeys...)
		return result
	}

	if root.key < startKey {
		return rangeQuery(root.right, startKey, endKey)
	}

	return rangeQuery(root.left, startKey, endKey)
}

func inorder(root *AVLNode) []*LSMDataEntry {
	if root == nil {
		return []*LSMDataEntry{}
	}
	return append(append(inorder(root.left), root.entry), inorder(root.right)...)
}

func preorder(root *AVLNode) []*LSMDataEntry {
	if root == nil {
		return []*LSMDataEntry{}
	}
	return append(append([]*LSMDataEntry{root.entry}, preorder(root.left)...), preorder(root.right)...)
}
