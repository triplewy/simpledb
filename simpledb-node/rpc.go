package main

import (
	"context"

	simpledb "github.com/triplewy/simpledb-embedded"
	pb "github.com/triplewy/simpledb/grpc"
)

// ReadRPC calls node's DB Read API
func (node *Node) ReadRPC(ctx context.Context, msg *pb.ReadMsg) (*pb.Entry, error) {
	entry, err := node.db.Read(msg.Key, msg.Fields)
	if err != nil {
		return nil, err
	}
	return &pb.Entry{
		Key:    entry.Key,
		Values: convertFields(entry.Fields),
	}, nil
}

// ScanRPC calls node's DB Scan API
func (node *Node) ScanRPC(ctx context.Context, msg *pb.ReadMsg) (*pb.EntriesMsg, error) {
	entries, err := node.db.Scan(msg.Key, msg.Fields)
	if err != nil {
		return nil, err
	}
	result := []*pb.Entry{}
	for _, entry := range entries {
		result = append(result, &pb.Entry{
			Key:    entry.Key,
			Values: convertFields(entry.Fields),
		})
	}
	return &pb.EntriesMsg{
		Entries: result,
	}, nil
}

// UpdateRPC calls node's DB Update API
func (node *Node) UpdateRPC(ctx context.Context, msg *pb.Entry) (*pb.OkMsg, error) {
	err := node.db.Update(msg.Key, convertPBFields(msg.Values))
	if err != nil {
		return &pb.OkMsg{Ok: false}, err
	}
	return &pb.OkMsg{Ok: true}, nil
}

// InsertRPC calls node's DB Insert API
func (node *Node) InsertRPC(ctx context.Context, msg *pb.Entry) (*pb.OkMsg, error) {
	err := node.db.Insert(msg.Key, convertPBFields(msg.Values))
	if err != nil {
		return &pb.OkMsg{Ok: false}, err
	}
	return &pb.OkMsg{Ok: true}, nil
}

// DeleteRPC calls node's DB Delete API
func (node *Node) DeleteRPC(ctx context.Context, msg *pb.KeyMsg) (*pb.OkMsg, error) {
	err := node.db.Delete(msg.Key)
	if err != nil {
		return &pb.OkMsg{Ok: false}, err
	}
	return &pb.OkMsg{Ok: true}, nil
}

func convertFields(fields map[string]*simpledb.Value) (result []*pb.Field) {
	for name, value := range fields {
		result = append(result, &pb.Field{
			Name:  name,
			Value: value.Data,
		})
	}
	return result
}

func convertPBFields(fields []*pb.Field) map[string][]byte {
	result := make(map[string][]byte)
	for _, field := range fields {
		result[field.Name] = field.Value
	}
	return result
}
