package db

type linkedListNode struct {
	next *linkedListNode
	prev *linkedListNode

	value interface{}
}

func (n *linkedListNode) Value() interface{} {
	return n.value
}

type linkedList struct {
	head *linkedListNode
	tail *linkedListNode
}

func newLinkedList() *linkedList {
	list := &linkedList{
		head: &linkedListNode{value: nil},
		tail: &linkedListNode{value: nil},
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (l *linkedList) Append(value interface{}) *linkedListNode {
	n := &linkedListNode{
		prev:  l.tail.prev,
		next:  l.tail,
		value: value,
	}

	l.tail.prev.next = n
	l.tail.prev = n

	return n
}

func (l *linkedList) Remove(n *linkedListNode) bool {
	if n == l.head || n == l.tail {
		return false
	}

	n.prev.next = n.next
	n.next.prev = n.prev

	return true
}

func (l *linkedList) Iterate() chan *linkedListNode {
	ch := make(chan *linkedListNode)

	go func() {
		n := l.head

		for n.next != l.tail {
			ch <- n.next
			n = n.next
		}

		close(ch)
	}()

	return ch
}

type orderedDict struct {
	lookup map[string]*linkedListNode
	list   *linkedList
	size   int
}

func newOrderedDict() *orderedDict {
	return &orderedDict{
		lookup: make(map[string]*linkedListNode),
		list:   newLinkedList(),
		size:   0,
	}
}

func (d *orderedDict) Set(key string, value interface{}) {
	if n, ok := d.lookup[key]; ok {
		n.value = value
	} else {
		d.lookup[key] = d.list.Append(value)
		d.size++
	}
}

func (d *orderedDict) Get(key string) (interface{}, bool) {
	if n, ok := d.lookup[key]; ok {
		return n.Value(), true
	}
	return nil, false
}

func (d *orderedDict) Remove(key string) bool {
	if n, ok := d.lookup[key]; ok {
		if ok := d.list.Remove(n); !ok {
			return false
		}
		delete(d.lookup, key)
		d.size--
		return true
	}
	return false
}

func (d *orderedDict) Iterate() chan interface{} {
	ch := make(chan interface{})

	go func() {
		for v := range d.list.Iterate() {
			ch <- v.Value()
		}

		close(ch)
	}()

	return ch
}
