package db

import (
	"sync"
)

// avlNode is a node in a AVL-Tree. For MVCC, each node may contain multiple lsmDataEntry
type avlNode struct {
	key     string
	entries []*Entry

	left   *avlNode
	right  *avlNode
	height int
}

// avlTree is struct for AVL-Tree
type avlTree struct {
	root *avlNode
	sync.RWMutex
}

func newAVLNode(entry *Entry) *avlNode {
	return &avlNode{
		key:     entry.Key,
		entries: []*Entry{entry},

		left:   nil,
		right:  nil,
		height: 1,
	}
}

// newavlTree creates a new AVL-Tree
func newAVLTree() *avlTree {
	return &avlTree{
		root: nil,
	}
}

// Put inserts a new node into an AVL-Tree
func (tree *avlTree) Put(entry *Entry) {
	tree.Lock()
	defer tree.Unlock()
	tree.root = put(tree.root, entry)
}

// Find finds the node in an AVL-Tree given a key. Returns error if key not found
func (tree *avlTree) Find(key string, ts uint64) *Entry {
	tree.RLock()
	defer tree.RUnlock()

	return find(tree.root, key, ts)
}

// Scan finds all nodes whose keys fall within the range query
func (tree *avlTree) Scan(keyRange *keyRange, ts uint64) []*Entry {
	tree.RLock()
	defer tree.RUnlock()
	node := commonParent(tree.root, keyRange)
	if node == nil {
		return []*Entry{}
	}
	return rangeQuery(node, keyRange, ts)
}

// Inorder prints inorder traversal of AVL-Tree
func (tree *avlTree) Inorder() []*Entry {
	tree.RLock()
	defer tree.RUnlock()
	return inorder(tree.root)
}

//Preorder prints keys of preorder traversal of AVL-Tree
func (tree *avlTree) Preorder() []string {
	tree.RLock()
	defer tree.RUnlock()
	pairs := preorder(tree.root)
	result := make([]string, len(pairs))
	for i, pair := range pairs {
		result[i] = pair.Key
	}
	return result
}

func put(root *avlNode, entry *Entry) *avlNode {
	if root == nil {
		return newAVLNode(entry)
	} else if entry.Key == root.key {
		root.entries = append([]*Entry{entry}, root.entries...)
		return root
	} else if entry.Key < root.key {
		root.left = put(root.left, entry)
	} else {
		root.right = put(root.right, entry)
	}

	root.height = 1 + max(getHeight(root.left), getHeight(root.right))
	balance := getBalance(root)

	// Case 1 - Left Left
	if balance > 1 && entry.Key < root.left.key {
		return rightRotate(root)
	}
	// Case 2 - Right Right
	if balance < -1 && entry.Key > root.right.key {
		return leftRotate(root)
	}
	// Case 3 - Left Right
	if balance > 1 && entry.Key > root.left.key {
		root.left = leftRotate(root.left)
		return rightRotate(root)
	}
	// Case 4 - Right Left
	if balance < -1 && entry.Key < root.right.key {
		root.right = rightRotate(root.right)
		return leftRotate(root)
	}

	return root
}

func getHeight(root *avlNode) int {
	if root == nil {
		return 0
	}
	return root.height
}

func getBalance(root *avlNode) int {
	if root == nil {
		return 0
	}
	return getHeight(root.left) - getHeight(root.right)
}

func leftRotate(z *avlNode) *avlNode {
	y := z.right
	T2 := y.left

	y.left = z
	z.right = T2

	z.height = 1 + max(getHeight(z.left), getHeight(z.right))
	y.height = 1 + max(getHeight(y.left), getHeight(y.right))

	return y
}

func rightRotate(z *avlNode) *avlNode {
	y := z.left
	T2 := y.right

	y.right = z
	z.left = T2

	z.height = 1 + max(getHeight(z.left), getHeight(z.right))
	y.height = 1 + max(getHeight(y.left), getHeight(y.right))

	return y
}

func find(root *avlNode, key string, ts uint64) *Entry {
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

func commonParent(root *avlNode, keyRange *keyRange) *avlNode {
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

func rangeQuery(root *avlNode, keyRange *keyRange, ts uint64) (entries []*Entry) {
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

func inorder(root *avlNode) (entries []*Entry) {
	if root == nil {
		return entries
	}
	entries = append(entries, inorder(root.left)...)
	entries = append(entries, root.entries...)
	entries = append(entries, inorder(root.right)...)
	return entries
}

func preorder(root *avlNode) (entries []*Entry) {
	if root == nil {
		return entries
	}
	entries = append(entries, root.entries...)
	entries = append(entries, preorder(root.left)...)
	entries = append(entries, preorder(root.right)...)
	return entries
}
