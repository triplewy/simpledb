package simpledb

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
)

var connByAddress = make(map[string]*grpc.ClientConn)
var connByAddressMutex = &sync.Mutex{}

func (node *Node) DelKVRPC(remote *RemoteNode, key string) (*pb.OkMsg, error) {
	if remote == nil {
		return &pb.OkMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.OkMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cc.DelKVCaller(ctx, &pb.KeyMsg{Key: key})
}

// GetHostInfoRPC is RPC for getting remote machine's stats
func (node *Node) GetHostInfoRPC(remote *RemoteNode) (*pb.HostStatsReplyMsg, error) {
	if remote == nil {
		return &pb.HostStatsReplyMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.HostStatsReplyMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cc.GetHostInfoCaller(ctx, &pb.Empty{})
}

func (node *Node) GetKVRPC(remote *RemoteNode, key string) (*pb.KeyValueMsg, error) {
	if remote == nil {
		return &pb.KeyValueMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.KeyValueMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cc.GetKVCaller(ctx, &pb.KeyMsg{Key: key})
}

func (node *Node) HeartbeatRPC(remote *RemoteNode) (*pb.OkMsg, error) {
	if remote == nil {
		return &pb.OkMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.OkMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := remoteNodesToMsg(node.Ring.Nodes)

	return cc.HeartbeatCaller(ctx, msg)
}

// JoinNodesRPC is RPC for joining simpledb cluster
func (node *Node) JoinNodesRPC(remote *RemoteNode) (*pb.RemoteNodesMsg, error) {
	if remote == nil {
		return &pb.RemoteNodesMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.RemoteNodesMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cc.JoinNodesCaller(ctx, &pb.RemoteNodeMsg{
		Addr:       node.RemoteSelf.Addr,
		Id:         node.RemoteSelf.ID,
		IsLeader:   node.RemoteSelf.isLeader,
		IsElection: node.RemoteSelf.isElection,
	})
}

func (node *Node) SetKVRPC(remote *RemoteNode, key, value string) (*pb.OkMsg, error) {
	if remote == nil {
		return &pb.OkMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.OkMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cc.SetKVCaller(ctx, &pb.KeyValueMsg{
		Key:   key,
		Value: value,
	})
}

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
