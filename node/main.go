package main

import (
	"flag"
	"fmt"
	"log"

	gossip "github.com/triplewy/gossip/gossip-node/src"
)

func main() {
	// addrPtr := flag.String("addr", "", "Address of a node in the Chord ring you wish to join")
	flag.Parse()

	node, err := gossip.CreateNode()

	if err != nil {
		fmt.Println("Unable to create new node!")
		log.Fatal(err)
	}

	fmt.Printf("Created Node: %v @ %v\n", node.ID, node.Addr)

	err = node.Server.Serve(node.Listener)

	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
