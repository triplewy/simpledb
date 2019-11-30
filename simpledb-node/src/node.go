package simpledb

import (
	"fmt"
	"log"
	"net"

	pb "github.com/triplewy/simpledb/grpc"
	db "github.com/triplewy/simpledb/simpledb-node/src/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Node represents database node
type Node struct {
	listener    net.Listener
	server      *grpc.Server
	serverCreds credentials.TransportCredentials
	clientCreds credentials.TransportCredentials

	db *db.DB
}

// NewNode creates a node with a gRPC server and database
func NewNode() (*Node, error) {
	node := new(Node)

	db, err := db.NewDB("/data/simpledb")
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("localhost:%d", RPCPort)
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

	server := grpc.NewServer(grpc.Creds(node.serverCreds))
	pb.RegisterSimpleDbServer(server, node)

	go func() {
		err := server.Serve(listener)
		if err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	node.listener = listener
	node.server = server
	node.serverCreds = serverCreds
	node.clientCreds = clientCreds
	node.db = db

	return node, nil
}
