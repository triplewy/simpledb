package db

type linkedListNode struct {
	next    *linkedListNode
	prev    *linkedListNode
	entries []*LSMDataEntry
}

type linkedList struct {
	head *linkedListNode
	tail *linkedListNode
}

func newLinkedList() *linkedList {
	list := &linkedList{
		head: &linkedListNode{entries: nil},
		tail: &linkedListNode{entries: nil},
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (l *linkedList) Append(entry *LSMDataEntry) *linkedListNode {
	n := &linkedListNode{
		prev:    l.tail.prev,
		next:    l.tail,
		entries: []*LSMDataEntry{entry},
	}

	l.tail.prev.next = n
	l.tail.prev = n
	return n
}

func (l *linkedList) Remove(n *linkedListNode) {
	if n == l.head || n == l.tail {
		return
	}
	n.prev.next = n.next
	n.next.prev = n.prev
}

type orderedDict struct {
	lookup map[string]*linkedListNode
	list   *linkedList
}

func newOrderedDict() *orderedDict {
	return &orderedDict{
		lookup: make(map[string]*linkedListNode),
		list:   newLinkedList(),
	}
}

func (d *orderedDict) Set(key string, entry *LSMDataEntry) {
	if n, ok := d.lookup[key]; ok {
		n.entries = append([]*LSMDataEntry{entry}, n.entries...)
	} else {
		d.lookup[key] = d.list.Append(entry)
	}
}

func (d *orderedDict) Get(key string) ([]*LSMDataEntry, bool) {
	if node, ok := d.lookup[key]; ok {
		return node.entries, true
	}
	return nil, false
}

func (d *orderedDict) Remove(key string) {
	if n, ok := d.lookup[key]; ok {
		d.list.Remove(n)
		delete(d.lookup, key)
	}
}

func (d *orderedDict) Iterate() (entries []*LSMDataEntry) {
	curr := d.list.head
	for curr.next != d.list.tail {
		entries = append(entries, curr.entries...)
		curr = curr.next
	}
	return entries
}
