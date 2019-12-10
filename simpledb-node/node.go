package main

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
	db "github.com/triplewy/simpledb-embedded"
	pb "github.com/triplewy/simpledb/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

// Node represents database node
type Node struct {
	Listener net.Listener
	Server   *grpc.Server

	serverCreds credentials.TransportCredentials
	clientCreds credentials.TransportCredentials

	db *db.DB

	RaftDir  string
	RaftBind string
	raft     *raft.Raft // The consensus mechanism
}

// NewNode creates a node with a gRPC server and database
func NewNode() (*Node, error) {
	node := new(Node)

	db, err := db.NewDB("data")
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("127.0.0.1:%d", RPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	serverCreds, err := credentials.NewServerTLSFromFile("../ssl/cert.pem", "../ssl/key.pem")
	if err != nil {
		return nil, err
	}
	clientCreds, err := credentials.NewClientTLSFromFile("../ssl/cert.pem", "")
	if err != nil {
		return nil, err
	}

	server := grpc.NewServer(grpc.Creds(serverCreds))
	pb.RegisterSimpleDbServer(server, node)

	node.Listener = listener
	node.Server = server
	node.serverCreds = serverCreds
	node.clientCreds = clientCreds
	node.db = db

	return node, nil
}
