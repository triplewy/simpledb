package simpledb

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

// ErrNodeNotFound thrown when remove node does not exist
var ErrNodeNotFound = errors.New("node not found")

// Ring is consistent hash data structure
type Ring struct {
	sync.Mutex
	Nodes RemoteNodes
}

// NewRing creates new ring
func NewRing() *Ring {
	return &Ring{Nodes: []*RemoteNode{}}
}

// AddNode adds node to ring
func (r *Ring) AddNode(node *RemoteNode) {
	r.Lock()
	defer r.Unlock()

	r.Nodes = append(r.Nodes, node)

	sort.Sort(r.Nodes)
}

// RemoveNode removes node from ring
func (r *Ring) RemoveNode(node *RemoteNode) error {
	r.Lock()
	defer r.Unlock()

	i := r.search(node.ID)
	if i >= r.Nodes.Len() || bytes.Compare(r.Nodes[i].ID, node.ID) != 0 {
		return ErrNodeNotFound
	}

	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

	return nil
}

// Get gets closest node given an id
func (r *Ring) Get(id string) *RemoteNode {
	hash := HashKey(id)
	i := r.search(hash)
	if i >= r.Nodes.Len() {
		i = 0
	}

	return r.Nodes[i]
}

func (r *Ring) search(id []byte) int {
	searchfn := func(i int) bool {
		return bytes.Compare(r.Nodes[i].ID, id) >= 0
	}

	return sort.Search(r.Nodes.Len(), searchfn)
}

// RemoteNodes is slice of type RemoteNode
type RemoteNodes []*RemoteNode

func (n RemoteNodes) Len() int           { return len(n) }
func (n RemoteNodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n RemoteNodes) Less(i, j int) bool { return bytes.Compare(n[i].ID, n[j].ID) == -1 }
