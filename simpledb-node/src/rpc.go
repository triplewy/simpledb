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

// JoinNodesCaller adds joining node to local consistent hash ring and replies with new ring
func (node *Node) JoinNodesCaller(ctx context.Context, remote *pb.RemoteNodeMsg) (*pb.RemoteNodesMsg, error) {
	node.Ring.AddNode(&RemoteNode{
		Addr:       remote.Addr,
		ID:         remote.Id,
		isLeader:   remote.IsLeader,
		isElection: remote.IsElection,
	})

	remoteNodes := remoteNodesToMsg(node.Ring.Nodes)

	return remoteNodes, nil
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

// GetHostInfoCaller returns it's current machine stats
func (node *Node) GetHostInfoCaller(ctx context.Context, in *pb.Empty) (*pb.HostStatsReplyMsg, error) {
	if node.stats.err != nil {
		return nil, node.stats.err
	}
	return &pb.HostStatsReplyMsg{
		TotalMemory: node.stats.totalMemory,
		UsedMemory:  node.stats.usedMemory,
		TotalSpace:  node.stats.totalSpace,
		UsedSpace:   node.stats.usedSpace,
		NumCores:    node.stats.numCores,
		CpuPercent:  node.stats.cpuPercent,
		Os:          node.stats.os,
		Hostname:    node.stats.hostname,
		Uptime:      node.stats.uptime,
	}, nil
}

// GetNodesCaller is for client use to get current ring of servers
func (node *Node) GetNodesCaller(ctx context.Context, in *pb.Empty) (*pb.RemoteNodesMsg, error) {
	remoteNodesMsg := remoteNodesToMsg(node.Ring.Nodes)

	return remoteNodesMsg, nil
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

func (node *Node) HeartbeatCaller(ctx context.Context, nodes *pb.RemoteNodesMsg) (*pb.OkMsg, error) {
	remoteNodes := msgToRemoteNodes(nodes)

	node.Ring.Nodes = remoteNodes

	return &pb.OkMsg{Ok: true}, nil
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
