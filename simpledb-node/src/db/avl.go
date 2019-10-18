package db

import (
	"errors"
	"sync"
)

type AVLNode struct {
	key   string
	value string

	left   *AVLNode
	right  *AVLNode
	height int
}

type AVLTree struct {
	root     *AVLNode
	capacity int
	size     int
	sync.RWMutex
}

type kvPair struct {
	key   string
	value string
}

func newAVLNode(key, value string) *AVLNode {
	return &AVLNode{
		key:    key,
		value:  value,
		left:   nil,
		right:  nil,
		height: 1,
	}
}

func NewAVLTree() *AVLTree {
	return &AVLTree{
		root:     nil,
		capacity: blockSize,
		size:     0,
	}
}

// Put inserts a new node into an AVL-Tree
func (tree *AVLTree) Put(key, value string) error {
	tree.Lock()
	defer tree.Unlock()

	newNode := newAVLNode(key, value)
	root, err := put(tree.root, newNode)
	if err != nil {
		return err
	}
	tree.root = root
	tree.size += 13 + len(key)

	return nil
}

// Find finds the node in an AVL-Tree given a key. Returns error if key not found
func (tree *AVLTree) Find(key string) (*AVLNode, error) {
	tree.RLock()
	defer tree.RUnlock()

	node, err := find(tree.root, key)
	if err != nil {
		return nil, err
	}
	return node, nil
}

//Inorder prints inorder traversal of AVL-Tree
func (tree *AVLTree) Inorder() []*kvPair {
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
func (tree *AVLTree) PreorderValues() []string {
	tree.RLock()
	defer tree.RUnlock()

	pairs := preorder(tree.root)
	result := make([]string, len(pairs))

	for i, pair := range pairs {
		result[i] = pair.value
	}

	return result
}

func put(root, newNode *AVLNode) (*AVLNode, error) {
	if root == nil {
		return newNode, nil
	} else if newNode.key == root.key {
		root.value = newNode.value
		return root, nil
	} else if newNode.key < root.key {
		node, err := put(root.left, newNode)
		if err != nil {
			return nil, err
		}
		root.left = node
	} else {
		node, err := put(root.right, newNode)
		if err != nil {
			return nil, err
		}
		root.right = node
	}

	root.height = 1 + Max(getHeight(root.left), getHeight(root.right))
	balance := getBalance(root)

	// Case 1 - Left Left
	if balance > 1 && newNode.key < root.left.key {
		return rightRotate(root), nil
	}
	// Case 2 - Right Right
	if balance < -1 && newNode.key > root.right.key {
		return leftRotate(root), nil
	}
	// Case 3 - Left Right
	if balance > 1 && newNode.key > root.left.key {
		root.left = leftRotate(root.left)
		return rightRotate(root), nil
	}
	// Case 4 - Right Left
	if balance < -1 && newNode.key < root.right.key {
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

func find(root *AVLNode, key string) (*AVLNode, error) {
	if root == nil {
		return nil, errors.New("Key not found")
	}
	if root.key == key {
		return root, nil
	}
	if key < root.key {
		return find(root.left, key)
	}
	return find(root.right, key)
}

func inorder(root *AVLNode) []*kvPair {
	if root == nil {
		return []*kvPair{}
	}
	return append(append(inorder(root.left), &kvPair{key: root.key, value: root.value}), inorder(root.right)...)
}

func preorder(root *AVLNode) []*kvPair {
	if root == nil {
		return []*kvPair{}
	}
	return append(append([]*kvPair{&kvPair{key: root.key, value: root.value}}, preorder(root.left)...), preorder(root.right)...)
}
