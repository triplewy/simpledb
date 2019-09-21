package simpledb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/triplewy/simpledb/simpledb-node/src/raft"
)

func (node *Node) run() {
	for {
		select {
		case err := <-node.rpcErrorChan:
			if err != nil {
				Error.Printf("RPC Server error :%v\n", err)
			}
			go node.runRPC(node.Server.Serve)
		case err := <-node.httpErrorChan:
			if err != nil {
				Error.Printf("HTTP Server error :%v\n", err)
			}
			go node.runHTTP(node.HTTPService.Start)
		case remote := <-node.transferChan:
			_, err := node.TransferKVRPC(remote)
			if err != nil {
				Error.Printf("TransferRPC error: %v\n", err)
			}
		case remote := <-node.electionChan:
			fmt.Println("Becoming raft node")

			os.MkdirAll(node.Config.RaftDir, 0700)
			node.raft.RaftDir = node.Config.RaftDir
			node.raft.RaftBind = ":" + node.Config.RaftPort

			err := node.raft.Open(false, string(node.RemoteSelf.ID[:]))
			if err != nil {
				log.Fatalf("failed to open raft at %s: %s", remote.HTTPAddr, err.Error())
			}

			h := raft.NewSvc(":"+node.Config.HttpPort, node.raft)
			node.HTTPService = h
			Out.Println("hraftd started successfully")
			go node.runHTTP(node.HTTPService.Start)

			err = join(remote.HTTPAddr, node.RemoteSelf.RaftAddr, node.RemoteSelf.RaftAddr)
			if err != nil {
				log.Fatalf("failed to join node at %s: %s", remote.HTTPAddr, err.Error())
			}
		}
	}
}

func join(joinAddr, raftAddr, nodeID string) error {
	fmt.Println(joinAddr, raftAddr)

	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	fmt.Println(resp)

	defer resp.Body.Close()

	return nil
}
