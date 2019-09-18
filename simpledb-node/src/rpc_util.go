package simpledb

import (
	"log"
	"sync"
	"time"

	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
)

var connByAddress = make(map[string]*grpc.ClientConn)
var connByAddressMutex = &sync.Mutex{}

func (node *Node) ClientConnection(remote *RemoteNode) (pb.SimpleDbClient, error) {
	connByAddressMutex.Lock()
	defer connByAddressMutex.Unlock()

	if cc, ok := connByAddress[remote.Addr]; ok && cc != nil {
		return pb.NewSimpleDbClient(cc), nil
	}

	cc, err := grpc.Dial(remote.Addr, grpc.WithTransportCredentials(node.clientCreds), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return nil, err
	}

	connByAddress[remote.Addr] = cc
	return pb.NewSimpleDbClient(cc), err
}

func pairsToMsg(pairs []*KV) *pb.KeyValuesMsg {
	keyValuesMsg := []*pb.KeyValueMsg{}

	for _, pair := range pairs {
		keyValuesMsg = append(keyValuesMsg, &pb.KeyValueMsg{
			Key:   pair.key,
			Value: pair.value,
		})
	}

	return &pb.KeyValuesMsg{Pairs: keyValuesMsg}
}

func remoteNodesToMsg(nodes []*RemoteNode) *pb.RemoteNodesMsg {
	remoteNodesMsg := []*pb.RemoteNodeMsg{}

	for _, node := range nodes {
		remoteNodesMsg = append(remoteNodesMsg, &pb.RemoteNodeMsg{
			Addr:       node.Addr,
			Id:         node.ID,
			IsLeader:   node.isLeader,
			IsElection: node.isElection,
		})
	}

	return &pb.RemoteNodesMsg{RemoteNodes: remoteNodesMsg}
}

func msgToPairs(msg *pb.KeyValuesMsg) []*KV {
	keyValues := []*KV{}

	for _, pair := range msg.Pairs {
		keyValues = append(keyValues, &KV{
			key:   pair.Key,
			value: pair.Value,
		})
	}

	return keyValues
}

func msgToRemoteNodes(nodes *pb.RemoteNodesMsg) []*RemoteNode {
	remoteNodes := []*RemoteNode{}

	for _, node := range nodes.RemoteNodes {
		remoteNodes = append(remoteNodes, &RemoteNode{
			Addr:       node.Addr,
			ID:         node.Id,
			isLeader:   node.IsLeader,
			isElection: node.IsElection,
		})
	}

	return remoteNodes
}
