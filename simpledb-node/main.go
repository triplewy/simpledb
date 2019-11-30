package main

import (
	"log"
	"os"
	"os/signal"

	simpledb "github.com/triplewy/simpledb/simpledb-node/src"
)

func main() {
	node := simpledb.NewNode()
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("simpledb exiting")
}
