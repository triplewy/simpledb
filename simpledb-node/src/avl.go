package simpledb

import (
	"errors"
	"sync"
)

type AVLNode struct {
	key      []byte
	numBytes int
	offset   int
	left     *AVLNode
	right    *AVLNode
	height   int
}

type AVLTree struct {
	root *AVLNode
	sync.RWMutex
}

func newAVLNode(key []byte, numBytes, offset int) *AVLNode {
	return &AVLNode{
		key:      key,
		numBytes: numBytes,
		offset:   offset,
		left:     nil,
		right:    nil,
		height:   1,
	}
}

func NewAVLTree() *AVLTree {
	return &AVLTree{
		root: nil,
	}
}

// Insert inserts a new node into an AVL-Tree
func (tree *AVLTree) Insert(key []byte, numBytes, offset int) error {
	tree.Lock()
	defer tree.Unlock()

	newNode := newAVLNode(key, numBytes, offset)
	root, err := insert(tree.root, newNode)
	if err != nil {
		return err
	}
	tree.root = root
	return nil
}

// Find finds the node in an AVL-Tree given a key. Returns error if key not found
func (tree *AVLTree) Find(key []byte) (*AVLNode, error) {
	tree.RLock()
	defer tree.RUnlock()

	node, err := find(tree.root, key)
	if err != nil {
		return nil, err
	}
	return node, nil
}

//Inorder prints inorder traversal of AVL-Tree
func (tree *AVLTree) Inorder() []string {
	tree.RLock()
	defer tree.RUnlock()

	return inorder(tree.root)
}

//Preorder prints preorder traversal of AVL-Tree
func (tree *AVLTree) Preorder() []string {
	tree.RLock()
	defer tree.RUnlock()

	return preorder(tree.root)
}

func insert(root, newNode *AVLNode) (*AVLNode, error) {
	if root == nil {
		return newNode, nil
	} else if bytesCompareEqual(newNode.key, root.key) {
		return nil, errors.New("Key already exists")
	} else if bytesCompareLess(newNode.key, root.key) {
		node, err := insert(root.left, newNode)
		if err != nil {
			return nil, err
		}
		root.left = node
	} else {
		node, err := insert(root.right, newNode)
		if err != nil {
			return nil, err
		}
		root.right = node
	}

	root.height = 1 + Max(getHeight(root.left), getHeight(root.right))
	balance := getBalance(root)

	// Case 1 - Left Left
	if balance > 1 && bytesCompareLess(newNode.key, root.left.key) {
		return rightRotate(root), nil
	}
	// Case 2 - Right Right
	if balance < -1 && bytesCompareGreater(newNode.key, root.right.key) {
		return leftRotate(root), nil
	}
	// Case 3 - Left Right
	if balance > 1 && bytesCompareGreater(newNode.key, root.left.key) {
		root.left = leftRotate(root.left)
		return rightRotate(root), nil
	}
	// Case 4 - Right Left
	if balance < -1 && bytesCompareLess(newNode.key, root.right.key) {
		root.right = rightRotate(root.right)
		return leftRotate(root), nil
	}

	return root, nil
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

	z.height = 1 + Max(getHeight(z.left), getHeight(z.right))
	y.height = 1 + Max(getHeight(y.left), getHeight(y.right))

	return y
}

func rightRotate(z *AVLNode) *AVLNode {
	y := z.left
	T2 := y.right

	y.right = z
	z.left = T2

	z.height = 1 + Max(getHeight(z.left), getHeight(z.right))
	y.height = 1 + Max(getHeight(y.left), getHeight(y.right))

	return y
}

func find(root *AVLNode, key []byte) (*AVLNode, error) {
	if root == nil {
		return nil, errors.New("Key not found")
	}
	if bytesCompareEqual(root.key, key) {
		return root, nil
	}
	if bytesCompareLess(key, root.key) {
		return find(root.left, key)
	}
	return find(root.right, key)
}

func inorder(root *AVLNode) []string {
	if root == nil {
		return []string{}
	}
	return append(append(inorder(root.left), string(root.key)), inorder(root.right)...)
}

func preorder(root *AVLNode) []string {
	if root == nil {
		return []string{}
	}
	return append(append([]string{string(root.key)}, preorder(root.left)...), preorder(root.right)...)
}
