package simpledb

import (
	"context"

	pb "github.com/triplewy/simpledb/grpc"
)

func (node *Node) DelKVCaller(ctx context.Context, msg *pb.KeyMsg) (*pb.OkMsg, error) {
	key := msg.Key
	remote := node.Ring.Get(key)
	if remote.Addr == node.RemoteSelf.Addr {
		node.store.del(key)
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
		val, err := node.store.get(key)
		if err != nil {
			return &pb.KeyValueMsg{}, err
		}
		return &pb.KeyValueMsg{
			Key:   key,
			Value: val,
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
	prev := node.Ring.Prev(node.RemoteSelf)
	remoteNodes := msgToRemoteNodes(nodes)
	node.Ring.Nodes = remoteNodes
	newPrev := node.Ring.Prev(node.RemoteSelf)

	if prev.Addr != newPrev.Addr {
		node.transferChan <- newPrev
	}

	return &pb.OkMsg{Ok: true}, nil
}

// JoinNodesCaller adds joining node to local consistent hash ring and replies with new ring
func (node *Node) JoinNodesCaller(ctx context.Context, remote *pb.RemoteNodeMsg) (*pb.RemoteNodesMsg, error) {
	prev := node.Ring.Prev(node.RemoteSelf)
	node.Ring.AddNode(&RemoteNode{
		Addr:       remote.Addr,
		ID:         remote.Id,
		isLeader:   remote.IsLeader,
		isElection: remote.IsElection,
	})
	newPrev := node.Ring.Prev(node.RemoteSelf)
	if prev.Addr != newPrev.Addr {
		node.transferChan <- newPrev
	}

	remoteNodes := remoteNodesToMsg(node.Ring.Nodes)
	return remoteNodes, nil
}

func (node *Node) SetKVCaller(ctx context.Context, msg *pb.KeyValueMsg) (*pb.OkMsg, error) {
	key := msg.Key
	value := msg.Value
	remote := node.Ring.Get(key)
	if remote.Addr == node.RemoteSelf.Addr {
		node.store.set(key, value)
		return &pb.OkMsg{Ok: true}, nil
	}

	return node.SetKVRPC(remote, key, value)
}

func (node *Node) TransferKVCaller(ctx context.Context, msg *pb.KeyValuesMsg) (*pb.OkMsg, error) {
	for _, pair := range msg.Pairs {
		node.store.set(pair.Key, pair.Value)
	}

	return &pb.OkMsg{Ok: true}, nil
}
