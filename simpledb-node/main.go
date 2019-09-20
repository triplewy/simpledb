package main

import (
	"flag"
	"fmt"
	"log"
	"net"
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

	fmt.Printf("Created Node: %x @ %s\n", node.ID, node.Addr)

	fmt.Printf("gRPC server now running @ %s\n", node.Addr)

	node.RunDiscovery()

	httpSvc, err := node.RunElection()
	if err != nil {
		log.Fatalf("unable to start raft: %v", err)
	}

	node.RunNormal()

	rpcErrorChan := make(chan error)
	httpErrorChan := make(chan error)

	go runRpc(node.Server.Serve, node.Listener, rpcErrorChan)
	go runHttp(httpSvc.Start, httpErrorChan)

	for {
		select {
		case err := <-rpcErrorChan:
			log.Printf("failed to serve rpc: %v", err)
		case err := <-httpErrorChan:
			log.Printf("failed to serve http: %v", err)
		}
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("simpledb exiting")
}

type serveRpc func(net.Listener) error

func runRpc(fn serveRpc, listener net.Listener, errChan chan error) {
	err := fn(listener)
	if err != nil {
		errChan <- err
	}
}

type serveHttp func() error

func runHttp(fn serveHttp, errChan chan error) {
	err := fn()
	if err != nil {
		errChan <- err
	}
}
