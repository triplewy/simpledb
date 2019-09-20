package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	_ "github.com/joho/godotenv/autoload"

	simpledb "github.com/triplewy/simpledb/simpledb-node/src"
)

var joinAddr string
var grpcPort string

func init() {
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&grpcPort, "port", "", "Set grpc port, default is 55001")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if grpcPort != "" {
		simpledb.GrpcPort = ":" + grpcPort
	}

	node, err := simpledb.CreateNode(joinAddr)
	if err != nil {
		log.Fatalf("unable to create new node: %v", err)
	}

	fmt.Printf("Created Node: %x @ %s\n", node.ID, node.Addr)

	err = node.Server.Serve(node.Listener)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("hraftd exiting")
}
