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

	Config *Config
	store  *store
	raft   *raft.Raft
}

// NewNode creates a node with a gRPC server and database
func NewNode(config *Config) (*Node, error) {
	node := new(Node)
	node.Config = config

	err := node.newStore()
	if err != nil {
		return nil, err
	}
	err = node.setupRPC()
	if err != nil {
		return nil, err
	}
	err = node.setupRaft()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (node *Node) setupRPC() error {
	addr := fmt.Sprintf("127.0.0.1:%d", node.Config.rpcPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	serverCreds, err := credentials.NewServerTLSFromFile(filepath.Join(node.Config.sslDir, "cert.pem"), filepath.Join(node.Config.sslDir, "key.pem"))
	if err != nil {
		return err
	}
	clientCreds, err := credentials.NewClientTLSFromFile(filepath.Join(node.Config.sslDir, "cert.pem"), "")
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

func (node *Node) setupRaft() error {
	// Get node outbound ip
	id, err := getOutboundIP()
	if err != nil {
		return err
	}
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id.String())

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", node.Config.raftPort))
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(fmt.Sprintf("localhost:%d", node.Config.raftPort), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}
	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(node.Config.dataDir, "snapshots"), retainSnapshotCount, os.Stderr)
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
