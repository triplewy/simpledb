package simpledb

import (
	"fmt"
	"time"
)

type Leader struct {
	replyChan chan bool
	failChan  chan *RemoteNode
}

func NewLeader() *Leader {
	return &Leader{
		replyChan: make(chan bool),
		failChan:  make(chan *RemoteNode),
	}
}

func (node *Node) leader() {
	heartbeat := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-heartbeat.C:
			node.sendHeartbeats()
		case reply := <-node.Leader.replyChan:
			fmt.Println(reply)
		case node := <-node.Leader.failChan:
			fmt.Println(node)
		}
	}
}

func (node *Node) sendHeartbeats() {
	for _, remoteNode := range node.Ring.Nodes {
		if remoteNode.Addr != node.RemoteSelf.Addr {
			fmt.Println("Sending heartbeat")
			go node.sendHeartbeat(remoteNode)
		}
	}
}

func (node *Node) sendHeartbeat(remote *RemoteNode) {
	reply, err := node.HeartbeatRPC(remote)
	if err != nil {
		Error.Printf("could not send HeartbeatRPC %v\n", err)
		node.Leader.failChan <- remote
	} else {
		node.Leader.replyChan <- reply.Ok
	}
}
