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

func (node *Node) JoinNodesRPC(remote *RemoteNode) (*pb.RemoteNodesReplyMsg, error) {
	if remote == nil {
		return &pb.RemoteNodesReplyMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.RemoteNodesReplyMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cc.JoinNodesCaller(ctx, &pb.RemoteNodeMsg{Addr: node.RemoteSelf.Addr, Id: node.RemoteSelf.ID})
}

func (node *Node) JoinNodesCaller(ctx context.Context, remote *pb.RemoteNodeMsg) (*pb.RemoteNodesReplyMsg, error) {
	node.Ring.AddNode(&RemoteNode{Addr: remote.Addr, ID: remote.Id})

	remoteNodes := []*pb.RemoteNodeMsg{}

	for _, node := range node.Ring.Nodes {
		remoteNodes = append(remoteNodes, &pb.RemoteNodeMsg{Addr: node.Addr, Id: node.ID})
	}

	return &pb.RemoteNodesReplyMsg{RemoteNodes: remoteNodes}, nil
}

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

func (node *Node) GetNodesCaller(ctx context.Context, in *pb.Empty) (*pb.RemoteNodesReplyMsg, error) {
	remoteNodes := []*pb.RemoteNodeMsg{}

	for _, node := range node.Ring.Nodes {
		remoteNodes = append(remoteNodes, &pb.RemoteNodeMsg{Addr: node.Addr, Id: node.ID})
	}

	return &pb.RemoteNodesReplyMsg{RemoteNodes: remoteNodes}, nil
}

func (node *Node) ClientConnection(remote *RemoteNode) (pb.SimpleDbClient, error) {
	connByAddressMutex.Lock()
	defer connByAddressMutex.Unlock()

	if cc, ok := connByAddress[remote.Addr]; ok && cc != nil {
		return pb.NewSimpleDbClient(cc), nil
	}

	cc, err := grpc.Dial(remote.Addr, grpc.WithTransportCredentials(node.clientCreds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return nil, err
	}

	connByAddress[remote.Addr] = cc
	return pb.NewSimpleDbClient(cc), err
}
