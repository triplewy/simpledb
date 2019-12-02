package main

import (
	"log"

	simpledb "github.com/triplewy/simpledb/simpledb-node/src"
)

func main() {
	node, err := simpledb.NewNode()
	if err != nil {
		log.Fatalf(err.Error())
	}
	if err := node.Server.Serve(node.Listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
