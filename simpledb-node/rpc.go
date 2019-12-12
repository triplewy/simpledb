package main

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"
	simpledb "github.com/triplewy/simpledb-embedded"
	pb "github.com/triplewy/simpledb/grpc"
)

// ReadRPC calls node's DB Read API
func (node *Node) ReadRPC(ctx context.Context, msg *pb.ReadMsg) (*pb.Entry, error) {
	c := &command{
		Op:             Read,
		Key:            msg.Key,
		ReadAttributes: msg.Attributes,
		WriteValues:    nil,
	}
	buf, err := encodeMsgPack(c)
	if err != nil {
		return nil, err
	}
	f := node.raft.Apply(buf.Bytes(), applyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}
	resp := f.Response().(*fsmReadResponse)
	if resp.err != nil {
		return nil, resp.err
	}
	attributes, err := valuesToAttributes(resp.entry.Attributes)
	if err != nil {
		return nil, err
	}
	return &pb.Entry{
		Key:        resp.entry.Key,
		Attributes: attributes,
	}, nil
}

// ScanRPC calls node's DB Scan API
func (node *Node) ScanRPC(ctx context.Context, msg *pb.ReadMsg) (*pb.EntriesMsg, error) {
	c := &command{
		Op:             Scan,
		Key:            msg.Key,
		ReadAttributes: msg.Attributes,
		WriteValues:    nil,
	}
	buf, err := encodeMsgPack(c)
	if err != nil {
		return nil, err
	}
	f := node.raft.Apply(buf.Bytes(), applyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}
	resp := f.Response().(*fsmScanResponse)
	if resp.err != nil {
		return nil, resp.err
	}
	result := []*pb.Entry{}
	for _, entry := range resp.entries {
		attributes, err := valuesToAttributes(entry.Attributes)
		if err != nil {
			return nil, err
		}
		result = append(result, &pb.Entry{
			Key:        entry.Key,
			Attributes: attributes,
		})
	}
	return &pb.EntriesMsg{
		Entries: result,
	}, nil
}

// UpdateRPC calls node's DB Update API
func (node *Node) UpdateRPC(ctx context.Context, msg *pb.Entry) (*pb.OkMsg, error) {
	values, err := attributesToValues(msg.Attributes)
	if err != nil {
		return nil, err
	}
	c := &command{
		Op:             Update,
		Key:            msg.Key,
		ReadAttributes: nil,
		WriteValues:    values,
	}
	buf, err := encodeMsgPack(c)
	if err != nil {
		return nil, err
	}
	f := node.raft.Apply(buf.Bytes(), applyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}
	resp := f.Response().(*fsmGenericResponse)
	if resp.err != nil {
		return nil, resp.err
	}
	return &pb.OkMsg{Ok: true}, nil
}

// InsertRPC calls node's DB Insert API
func (node *Node) InsertRPC(ctx context.Context, msg *pb.Entry) (*pb.OkMsg, error) {
	values, err := attributesToValues(msg.Attributes)
	if err != nil {
		return nil, err
	}
	c := &command{
		Op:             Insert,
		Key:            msg.Key,
		ReadAttributes: nil,
		WriteValues:    values,
	}
	buf, err := encodeMsgPack(c)
	if err != nil {
		return nil, err
	}
	f := node.raft.Apply(buf.Bytes(), applyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}
	resp := f.Response().(*fsmGenericResponse)
	if resp.err != nil {
		return nil, resp.err
	}
	return &pb.OkMsg{Ok: true}, nil
}

// DeleteRPC calls node's DB Delete API
func (node *Node) DeleteRPC(ctx context.Context, msg *pb.KeyMsg) (*pb.OkMsg, error) {
	c := &command{
		Op:             Delete,
		Key:            msg.Key,
		ReadAttributes: nil,
		WriteValues:    nil,
	}
	buf, err := encodeMsgPack(c)
	if err != nil {
		return nil, err
	}
	f := node.raft.Apply(buf.Bytes(), applyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}
	resp := f.Response().(*fsmGenericResponse)
	if resp.err != nil {
		return nil, resp.err
	}
	return &pb.OkMsg{Ok: true}, nil
}

func valuesToAttributes(fields map[string]*simpledb.Value) (result []*pb.Attribute, err error) {
	for name, value := range fields {
		attribute := &pb.Attribute{
			Name:  name,
			Value: value.Data,
		}
		switch value.DataType {
		case simpledb.Bool:
			attribute.Type = pb.Attribute_BOOL
		case simpledb.Int:
			attribute.Type = pb.Attribute_INT
		case simpledb.Uint:
			attribute.Type = pb.Attribute_UINT
		case simpledb.Float:
			attribute.Type = pb.Attribute_FLOAT
		case simpledb.String:
			attribute.Type = pb.Attribute_STRING
		case simpledb.Bytes:
			attribute.Type = pb.Attribute_STRING
		default:
			return nil, fmt.Errorf("field contains invalid DataType: %v", value.DataType)
		}
		result = append(result, attribute)
	}
	return result, nil
}

func attributesToValues(attributes []*pb.Attribute) (map[string]*simpledb.Value, error) {
	values := make(map[string]*simpledb.Value)
	for _, attribute := range attributes {
		value := &simpledb.Value{
			Data: attribute.Value,
		}
		switch attribute.Type {
		case pb.Attribute_BOOL:
			value.DataType = simpledb.Bool
		case pb.Attribute_INT:
			value.DataType = simpledb.Int
		case pb.Attribute_UINT:
			value.DataType = simpledb.Uint
		case pb.Attribute_FLOAT:
			value.DataType = simpledb.Float
		case pb.Attribute_STRING:
			value.DataType = simpledb.String
		case pb.Attribute_BYTES:
			value.DataType = simpledb.Bytes
		default:
			return nil, fmt.Errorf("attribute contains invalid type: %v", attribute.Type)
		}
		values[attribute.Name] = value
	}
	return values, nil
}
