package simpledb

import (
	"context"

	pb "github.com/triplewy/simpledb/grpc"
)

func (node *Node) DelKVCaller(ctx context.Context, msg *pb.KeyMsg) (*pb.OkMsg, error) {
	key := msg.Key
	remote := node.Ring.Get(key)
	if remote.Addr == node.RemoteSelf.Addr {
		node.DsLock.Lock()
		defer node.DsLock.Unlock()
		delete(node.dataStore, key)

		return &pb.OkMsg{Ok: true}, nil
	}

	return node.DelKVRPC(remote, key)
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

func (node *Node) GetKVCaller(ctx context.Context, msg *pb.KeyMsg) (*pb.KeyValueMsg, error) {
	key := msg.Key
	remote := node.Ring.Get(key)
	if remote.Addr == node.RemoteSelf.Addr {
		return &pb.KeyValueMsg{
			Key:   key,
			Value: node.dataStore[key],
		}, nil
	}
	return node.GetKVRPC(remote, key)
}

// GetNodesCaller is for client use to get current ring of servers
func (node *Node) GetNodesCaller(ctx context.Context, in *pb.Empty) (*pb.RemoteNodesMsg, error) {
	remoteNodesMsg := remoteNodesToMsg(node.Ring.Nodes)

	return remoteNodesMsg, nil
}

func (node *Node) HeartbeatCaller(ctx context.Context, nodes *pb.RemoteNodesMsg) (*pb.OkMsg, error) {
	remoteNodes := msgToRemoteNodes(nodes)

	node.Ring.Nodes = remoteNodes

	return &pb.OkMsg{Ok: true}, nil
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

func (node *Node) SetKVCaller(ctx context.Context, msg *pb.KeyValueMsg) (*pb.OkMsg, error) {
	key := msg.Key
	value := msg.Value
	remote := node.Ring.Get(key)
	if remote.Addr == node.RemoteSelf.Addr {
		node.DsLock.Lock()
		defer node.DsLock.Unlock()
		node.dataStore[key] = value

		return &pb.OkMsg{Ok: true}, nil
	}

	return node.SetKVRPC(remote, key, value)
}
