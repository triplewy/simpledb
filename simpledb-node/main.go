package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
)

var dir string
var rpcPort int
var raftPort int

func init() {
	flag.StringVar(&dir, "d", "/tmp/simpledb", "data directory for simpleDB")
	flag.IntVar(&rpcPort, "rpc", 30000, "rpc port for node")
	flag.IntVar(&raftPort, "raft", 30001, "raft port for node")
}
func main() {
	flag.Parse()

	_, err := NewNode(dir, rpcPort, raftPort)
	if err != nil {
		log.Fatalf(err.Error())
	}

	log.Println("SimpleDB started successfully")
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("SimpleDB exiting")
}
