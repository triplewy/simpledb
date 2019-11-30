package simpledb

import (
	"context"

	pb "github.com/triplewy/simpledb/grpc"
)

// ReadRPC ...
func (node *Node) ReadRPC(ctx context.Context, msg *pb.ReadMsg) (*pb.ValuesMsg, error) {
	err := node.db.View(func(txn *Txn) error {
		txn.
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// ScanRPC ...
func (node *Node) ScanRPC(ctx context.Context, msg *pb.ReadMsg) (*pb.ValuesMsg, error) {
	return nil, nil
}

// UpdateRPC returns it's current machine stats
func (node *Node) UpdateRPC(ctx context.Context, msg *pb.WriteMsg) (*pb.OkMsg, error) {
	return nil, nil
}

// InsertRPC ...
func (node *Node) InsertRPC(ctx context.Context, msg *pb.WriteMsg) (*pb.OkMsg, error) {
	return nil, nil
}

// DeleteRPC is for client use to get current ring of servers
func (node *Node) DeleteRPC(ctx context.Context, msg *pb.KeyMsg) (*pb.OkMsg, error) {
	return nil, nil
}
