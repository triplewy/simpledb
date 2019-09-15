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

func (node *Node) ClientConnection(remote *RemoteNode) (pb.SimpleDbClient, error) {
	connByAddressMutex.Lock()
	defer connByAddressMutex.Unlock()

	if cc, ok := connByAddress[remote.Addr]; ok && cc != nil {
		return pb.NewSimpleDbClient(cc), nil
	}

	cc, err := grpc.Dial(remote.Addr, grpc.WithTransportCredentials(node.creds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	if err != nil {
		return nil, err
	}

	connByAddress[remote.Addr] = cc
	return pb.NewSimpleDbClient(cc), err
}
