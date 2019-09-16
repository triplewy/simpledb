package simpledb

import (
	"testing"
)

// TestAddNode tests Ring AddNode
func TestAddNode(t *testing.T) {
	ring := NewRing()

	self := new(RemoteNode)
	self.Addr = GetOutboundIP().String()
	self.ID = HashKey(self.Addr)

	ring.AddNode(self)
	if ring.Nodes.Len() != 1 {
		t.Fatalf("failed to add node")
	}

	ring.AddNode(&RemoteNode{Addr: "127.0.0.1:55001", ID: HashKey("127.0.0.1:55001")})
	if ring.Nodes.Len() != 2 {
		t.Fatalf("failed to add node")
	}
}

// TestRemoveNode tests removing node from ring
func TestRemoveNode(t *testing.T) {
	ring := NewRing()

	self := new(RemoteNode)
	self.Addr = GetOutboundIP().String()
	self.ID = HashKey(self.Addr)

	ring.AddNode(self)
	if ring.Nodes.Len() != 1 {
		t.Fatalf("failed to add node")
	}

	ring.RemoveNode(self)
	if ring.Nodes.Len() != 0 {
		t.Fatalf("failed to remove node")
	}
}

// TestGet tests finding node for a key
func TestGet(t *testing.T) {
	ring := NewRing()

	self := new(RemoteNode)
	self.Addr = GetOutboundIP().String()
	self.ID = HashKey(self.Addr)

	ring.AddNode(self)
	if ring.Nodes.Len() != 1 {
		t.Fatalf("failed to add node")
	}

	ring.AddNode(&RemoteNode{Addr: "something", ID: HashKey("something")})
	if ring.Nodes.Len() != 2 {
		t.Fatalf("failed to add node")
	}

	node := ring.Get("test")

	if node != ring.Nodes[0] {
		t.Fatalf("failed to get correct node")
	}

}
