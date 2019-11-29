package db

import (
	"sync"
)

// AVLNode is a node in a AVL-Tree. For MVCC, each node may contain multiple LSMDataEntry
type AVLNode struct {
	key     string
	entries []*LSMDataEntry

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
		key:     entry.key,
		entries: []*LSMDataEntry{entry},

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
	tree.root = put(tree.root, entry)
}

// Find finds the node in an AVL-Tree given a key. Returns error if key not found
func (tree *AVLTree) Find(key string, ts uint64) *LSMDataEntry {
	tree.RLock()
	defer tree.RUnlock()

	return find(tree.root, key, ts)
}

// Range finds all nodes whose keys fall within the range query
func (tree *AVLTree) Range(keyRange *KeyRange, ts uint64) []*LSMDataEntry {
	tree.RLock()
	defer tree.RUnlock()
	node := commonParent(tree.root, keyRange)
	if node == nil {
		return []*LSMDataEntry{}
	}
	return rangeQuery(node, keyRange, ts)
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

func put(root *AVLNode, entry *LSMDataEntry) *AVLNode {
	if root == nil {
		return newAVLNode(entry)
	} else if entry.key == root.key {
		root.entries = append([]*LSMDataEntry{entry}, root.entries...)
		return root
	} else if entry.key < root.key {
		root.left = put(root.left, entry)
	} else {
		root.right = put(root.right, entry)
	}

	root.height = 1 + max(getHeight(root.left), getHeight(root.right))
	balance := getBalance(root)

	// Case 1 - Left Left
	if balance > 1 && entry.key < root.left.key {
		return rightRotate(root)
	}
	// Case 2 - Right Right
	if balance < -1 && entry.key > root.right.key {
		return leftRotate(root)
	}
	// Case 3 - Left Right
	if balance > 1 && entry.key > root.left.key {
		root.left = leftRotate(root.left)
		return rightRotate(root)
	}
	// Case 4 - Right Left
	if balance < -1 && entry.key < root.right.key {
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

func find(root *AVLNode, key string, ts uint64) *LSMDataEntry {
	if root == nil {
		return nil
	}
	if root.key == key {
		for _, entry := range root.entries {
			if entry.ts < ts {
				return entry
			}
		}
		return nil
	}
	if key < root.key {
		return find(root.left, key, ts)
	}
	return find(root.right, key, ts)
}

func commonParent(root *AVLNode, keyRange *KeyRange) *AVLNode {
	startKey := keyRange.startKey
	endKey := keyRange.endKey
	if root == nil {
		return nil
	}
	if startKey < root.key && endKey < root.key {
		return commonParent(root.left, keyRange)
	}
	if startKey > root.key && endKey > root.key {
		return commonParent(root.right, keyRange)
	}
	return root
}

func rangeQuery(root *AVLNode, keyRange *KeyRange, ts uint64) (entries []*LSMDataEntry) {
	startKey := keyRange.startKey
	endKey := keyRange.endKey
	if root == nil {
		return entries
	}
	if startKey <= root.key && root.key <= endKey {
		leftKeys := rangeQuery(root.left, keyRange, ts)
		rightKeys := rangeQuery(root.right, keyRange, ts)

		entries = append(entries, leftKeys...)
		for _, entry := range root.entries {
			if entry.ts < ts {
				entries = append(entries, entry)
				break
			}
		}
		entries = append(entries, rightKeys...)
		return entries
	}

	if root.key < startKey {
		return rangeQuery(root.right, keyRange, ts)
	}

	return rangeQuery(root.left, keyRange, ts)
}

func inorder(root *AVLNode) (entries []*LSMDataEntry) {
	if root == nil {
		return entries
	}
	entries = append(entries, inorder(root.left)...)
	entries = append(entries, root.entries...)
	entries = append(entries, inorder(root.right)...)
	return entries
}

func preorder(root *AVLNode) (entries []*LSMDataEntry) {
	if root == nil {
		return entries
	}
	entries = append(entries, root.entries...)
	entries = append(entries, preorder(root.left)...)
	entries = append(entries, preorder(root.right)...)
	return entries
}
