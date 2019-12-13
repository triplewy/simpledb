package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
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

	dir   string
	store *store
	raft  *raft.Raft
}

// NewNode creates a node with a gRPC server and database
func NewNode(directory string, rpcPort, raftPort int) (*Node, error) {
	node := new(Node)
	node.dir = directory

	err := node.newStore(node.dir)
	if err != nil {
		return nil, err
	}
	err = node.setupRPC(rpcPort)
	if err != nil {
		return nil, err
	}
	err = node.setupRaft(raftPort)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (node *Node) setupRPC(port int) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	serverCreds, err := credentials.NewServerTLSFromFile("ssl/cert.pem", "ssl/key.pem")
	if err != nil {
		return err
	}
	clientCreds, err := credentials.NewClientTLSFromFile("ssl/cert.pem", "")
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.Creds(serverCreds))
	pb.RegisterSimpleDbServer(server, node)

	node.Listener = listener
	node.Server = server
	node.serverCreds = serverCreds
	node.clientCreds = clientCreds

	go func() {
		if err := node.Server.Serve(node.Listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return nil
}

func (node *Node) setupRaft(port int) error {
	// Get node outbound ip
	id, err := getOutboundIP()
	if err != nil {
		return err
	}
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id.String())

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(fmt.Sprintf("localhost:%d", port), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}
	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(node.dir, "snapshots"), retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	// Create raft node
	ra, err := raft.NewRaft(config, node.store, node.store, node.store, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	node.raft = ra

	// Do discovery here
	// if enableSingle {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	ra.BootstrapCluster(configuration)
	// }

	return nil
}
