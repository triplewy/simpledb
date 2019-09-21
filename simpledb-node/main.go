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

var grpcPort string
var httpPort string
var raftPort string

func init() {
	flag.StringVar(&grpcPort, "grpc", "30000", "Set grpc port, default is 30000")
	flag.StringVar(&httpPort, "http", "40000", "Set grpc port, default is 40000")
	flag.StringVar(&raftPort, "raft", "50000", "Set grpc port, default is 50000")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	node := simpledb.CreateNode()

	node.Config.RpcPort = grpcPort
	node.Config.HttpPort = httpPort
	node.Config.RaftPort = raftPort

	err := node.CreateServer()
	if err != nil {
		log.Fatalf("unable to start server: %v", err)
	}

	fmt.Printf("Created Node: %x @ %s\n", node.RemoteSelf.ID, node.RemoteSelf.Addr)

	fmt.Printf("gRPC server now running @ %s\n", node.RemoteSelf.Addr)

	node.RunDiscovery()
	err = node.RunElection()
	if err != nil {
		log.Fatalf("unable to run election: %v", err)
	}

	// httpSvc, err := node.RunElection()
	// if err != nil {
	// 	log.Fatalf("unable to start raft: %v", err)
	// }

	node.RunNormal()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("simpledb exiting")
}
