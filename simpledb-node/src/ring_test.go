package simpledb

import (
	"fmt"
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

	ring.AddNode(&RemoteNode{Addr: "localhost:50051", ID: HashKey("localhost:50051")})
	if ring.Nodes.Len() != 1 {
		t.Fatalf("failed to add node")
	}

	ring.AddNode(&RemoteNode{Addr: "localhost:50052", ID: HashKey("localhost:50052")})
	if ring.Nodes.Len() != 2 {
		t.Fatalf("failed to add node")
	}

	for _, node := range ring.Nodes {
		fmt.Printf("addr: %s, id: %x\n", node.Addr, node.ID)
	}

	fmt.Printf("test: %x\n", HashKey("test"))

	node := ring.Get("test")

	if node.Addr != "localhost:50052" {
		t.Fatalf("failed to get correct node")
	}

}

// TestPrev tests finding next node in ring
func TestPrev(t *testing.T) {
	ring := NewRing()

	ring.AddNode(&RemoteNode{Addr: "localhost:50051", ID: HashKey("localhost:50051")})
	if ring.Nodes.Len() != 1 {
		t.Fatalf("failed to add node")
	}

	ring.AddNode(&RemoteNode{Addr: "localhost:50052", ID: HashKey("localhost:50052")})
	if ring.Nodes.Len() != 2 {
		t.Fatalf("failed to add node")
	}

	ring.AddNode(&RemoteNode{Addr: "localhost:50053", ID: HashKey("localhost:50053")})
	if ring.Nodes.Len() != 3 {
		t.Fatalf("failed to add node")
	}

	for _, node := range ring.Nodes {
		fmt.Printf("Addr: %s, ID: %x\n", node.Addr, node.ID)
	}

	node := ring.Prev(&RemoteNode{Addr: "localhost:50051", ID: HashKey("localhost:50051")})
	if node.Addr != "localhost:50053" {
		fmt.Printf("Addr: %s, ID: %x\n", node.Addr, node.ID)
		t.Fatalf("failed to get correct prev node")
	}

	node = ring.Prev(&RemoteNode{Addr: "localhost:50053", ID: HashKey("localhost:50053")})
	if node.Addr != "localhost:50052" {
		fmt.Printf("Addr: %s, ID: %x\n", node.Addr, node.ID)
		t.Fatalf("failed to get correct prev node")
	}
}
