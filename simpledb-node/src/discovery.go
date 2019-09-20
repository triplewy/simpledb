package simpledb

import (
	"sync"
	"time"
)

// Discovery is struct for discovering nodes upon cluster startup
type Discovery struct {
	nodes []*RemoteNode
	sync.WaitGroup
}

// NewDiscovery creates new Discovery struct
func NewDiscovery() *Discovery {
	return &Discovery{
		nodes: []*RemoteNode{},
	}
}

func (node *Node) runDiscovery() {
	Out.Println("Entering Discovery Phase!")

	timeoutTimer := time.NewTimer(time.Duration(node.Config.DiscoveryTimeout) * time.Second)
	discoveryTicker := time.NewTicker(4 * time.Second)

	if len(node.Config.DiscoveryAddrs) == 1 && node.Config.DiscoveryAddrs[0] == "" {
		return
	}

	nodes := []*RemoteNode{}
	for _, addr := range node.Config.DiscoveryAddrs {
		remote := &RemoteNode{
			Addr: addr,
			ID:   HashKey(addr),
		}
		nodes = append(nodes, remote)
	}

	node.Discovery.nodes = nodes

	for {
		select {
		case <-timeoutTimer.C:
			node.Discovery.Wait()
			Out.Println("WE'RE EXITING")
			return
		case <-discoveryTicker.C:
			Out.Println("Sending discovery")
			node.sendDiscoveries()
		}
	}

}

func (node *Node) sendDiscoveries() {
	for _, remote := range node.Discovery.nodes {
		if remote.Addr != node.RemoteSelf.Addr {
			Out.Printf("%s\n", remote.Addr)
			node.Discovery.Add(1)
			go node.sendDiscovery(remote)
		}
	}

	for _, remote := range node.Ring.Nodes {
		if remote.Addr != node.RemoteSelf.Addr {
			Out.Printf("%s\n", remote.Addr)
			node.Discovery.Add(1)
			go node.sendDiscovery(remote)
		}
	}
}

func (node *Node) sendDiscovery(remote *RemoteNode) {
	defer node.Discovery.Done()

	reply, err := node.DiscoverNodesRPC(remote)
	if err != nil {
		Error.Printf("sendDiscovery error %v\n", err)
		return
	}
	nodes := msgToRemoteNodes(reply)
	node.Ring.Union(nodes)
}
