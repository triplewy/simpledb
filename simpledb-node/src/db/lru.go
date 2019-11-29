package db

type linkedListNode struct {
	next *linkedListNode
	prev *linkedListNode
	key  string
	ts   uint64
}

type doublyLinkedList struct {
	head *linkedListNode
	tail *linkedListNode
}

func newDoublyLinkedList() *doublyLinkedList {
	list := &doublyLinkedList{
		head: &linkedListNode{},
		tail: &linkedListNode{},
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (list *doublyLinkedList) Append(node *linkedListNode) {
	node.next = list.tail
	node.prev = list.tail.prev
	list.tail.prev.next = node
	list.tail.prev = node
}

func (list *doublyLinkedList) Remove(node *linkedListNode) {
	if node == list.head || node == list.tail {
		return
	}
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (list *doublyLinkedList) Pop() (string, uint64) {
	if list.head.next == list.tail {
		return "", 0
	}
	node := list.head.next
	key := node.key
	ts := node.ts
	list.head.next = node.next
	node.next.prev = list.head
	return key, ts
}

type lru struct {
	lookup   map[string]*linkedListNode
	list     *doublyLinkedList
	size     int
	capacity int
	maxTs    uint64
}

func newLRU(capacity int) *lru {
	return &lru{
		lookup:   make(map[string]*linkedListNode),
		list:     newDoublyLinkedList(),
		size:     0,
		capacity: capacity,
		maxTs:    0,
	}
}

func (lru *lru) Insert(key string, ts uint64) {
	if node, ok := lru.lookup[key]; ok {
		node.key = key
		node.ts = ts
		lru.list.Remove(node)
		lru.list.Append(node)
		return
	}
	if lru.size == lru.capacity {
		delKey, delTS := lru.list.Pop()
		delete(lru.lookup, delKey)
		if delTS > lru.maxTs {
			lru.maxTs = delTS
		}
		lru.size--
	}
	node := &linkedListNode{key: key, ts: ts}
	lru.lookup[key] = node
	lru.list.Append(node)
	lru.size++
}

func (lru *lru) Get(key string) (uint64, bool) {
	if node, ok := lru.lookup[key]; ok {
		return node.ts, true
	}
	return 0, false
}
