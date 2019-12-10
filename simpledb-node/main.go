package main

import (
	"log"
)

func main() {
	node, err := NewNode()
	if err != nil {
		log.Fatalf(err.Error())
	}
	if err := node.Server.Serve(node.Listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
