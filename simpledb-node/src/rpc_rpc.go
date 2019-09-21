package simpledb

import (
	"context"
	"errors"
	"time"

	pb "github.com/triplewy/simpledb/grpc"
)

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

func (node *Node) DiscoverNodesRPC(remote *RemoteNode) (*pb.RemoteNodesMsg, error) {
	if remote == nil {
		return &pb.RemoteNodesMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.RemoteNodesMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := remoteNodesToMsg(node.Ring.Nodes)
	return cc.DiscoverNodesCaller(ctx, msg)
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

	return cc.JoinNodesCaller(ctx, remoteNodeToMsg(node.RemoteSelf))
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

func (node *Node) TransferKVRPC(remote *RemoteNode) (*pb.OkMsg, error) {
	if remote == nil {
		return &pb.OkMsg{}, errors.New("remoteNode is empty")
	}

	cc, err := node.ClientConnection(remote)
	if err != nil {
		return &pb.OkMsg{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pairs := node.store.transfer(remote.ID)
	msg := pairsToMsg(pairs)

	reply, err := cc.TransferKVCaller(ctx, msg)
	if err != nil {
		return &pb.OkMsg{}, err
	}

	for _, pair := range pairs {
		node.store.del(pair.key)
	}

	return reply, nil
}
