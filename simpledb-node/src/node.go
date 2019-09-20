package simpledb

import (
	"net"
	"os"
	"sync"

	pb "github.com/triplewy/simpledb/grpc"
	"github.com/triplewy/simpledb/simpledb-node/src/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// DiscoveryState represents state when node is discovering other nodes
const (
	DiscoveryState uint64 = iota
	ElectionState
	NormalState
)

// KeyLength is number of bits (i.e. m value). Assumes <= 128 and divisible by 8
const KeyLength = 16

// RemoteNode represents a Non-local node.
type RemoteNode struct {
	ID         []byte
	Addr       string
	isLeader   bool
	isElection bool
}

// Node represents data structures stored in a gossip node.
type Node struct {
	ID         []byte      /* Unique Node Id */
	Addr       string      /* String of listener address */
	RemoteSelf *RemoteNode /* Remote node of our self */

	Listener    net.Listener /* Node listener socket */
	Server      *grpc.Server /* RPC Server */
	serverCreds credentials.TransportCredentials
	clientCreds credentials.TransportCredentials

	IsShutdown bool           /* Is node in process of shutting down? */
	sdLock     sync.RWMutex   /* RWLock for shutdown flag */
	wg         sync.WaitGroup /* WaitGroup of concurrent goroutines to sync before exiting */

	stats     *Stats
	store     *Store
	raft      *raft.Election
	Ring      *Ring
	Leader    *Leader
	Config    *Config
	Discovery *Discovery
	state     uint64

	transferChan chan *RemoteNode
}

// CreateNode creates a Gossip node with random ID based on listener address.
func CreateNode() *Node {
	node := new(Node)
	node.init()
	go node.runGetStats()
	return node
}

// CreateServer creates and starts gRPC server for node
func (node *Node) CreateServer() error {
	// If on Brown network
	// node.Addr = GetOutboundIP().String
	// If on local computer
	node.Addr = "localhost:" + node.Config.RpcPort
	node.ID = HashKey(node.Addr)
	node.RemoteSelf = &RemoteNode{
		Addr: node.Addr,
		ID:   node.ID,
	}
	node.Ring.AddNode(node.RemoteSelf)

	listener, err := net.Listen("tcp", ":"+node.Config.RpcPort)
	if err != nil {
		Error.Printf("failed to listen: %v", err)
		return err
	}
	node.Listener = listener

	serverCreds, err := credentials.NewServerTLSFromFile("../ssl/cert.pem", "../ssl/key.pem")
	if err != nil {
		Error.Printf("could not create credentials: %v", err)
		return err
	}
	clientCreds, err := credentials.NewClientTLSFromFile("../ssl/cert.pem", "")
	if err != nil {
		Error.Printf("could not create credentials: %v", err)
		return err
	}

	node.serverCreds = serverCreds
	node.clientCreds = clientCreds
	node.Server = grpc.NewServer(grpc.Creds(node.serverCreds))

	pb.RegisterSimpleDbServer(node.Server, node)
	return nil
}

// RunDiscovery commences discovery phase of node
func (node *Node) RunDiscovery() {
	node.state = DiscoveryState
	node.runDiscovery()
}

// RunElection commences election phase of node
func (node *Node) RunElection() (*raft.Service, error) {
	node.state = ElectionState

	if node.Ring.Nodes.Len() > 1 {

	} else {
		node.RemoteSelf.isLeader = true
		node.RemoteSelf.isElection = true
		err := node.Ring.RemoveNode(node.RemoteSelf)
		if err != nil {
			return nil, err
		}
		node.Ring.AddNode(node.RemoteSelf)

		os.MkdirAll(node.Config.RaftDir, 0700)

		node.raft.RaftDir = node.Config.RaftDir
		node.raft.RaftBind = ":" + node.Config.RaftPort

		err = node.raft.Open(true, string(node.RemoteSelf.ID[:]))
		if err != nil {
			return nil, err
		}

		h := raft.NewSvc(":"+node.Config.HttpPort, node.raft)

		Out.Println("hraftd started successfully")
		return h, nil
		// If join was specified, make the join request.
		// if joinAddr != "" {
		// 	if err := join(joinAddr, raftAddr, nodeID); err != nil {
		// 		log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		// 	}
		// }
	}

	return nil, nil

	// var leader *RemoteNode
	// numElection := 0

	// if joinAddr != "" {
	// 	reply, err := node.JoinNodesRPC(&RemoteNode{Addr: joinAddr, ID: HashKey(joinAddr)})
	// 	if err != nil {
	// 		log.Fatalf("failed to join node %v", err)
	// 	}
	// 	ring := new(Ring)
	// 	for _, node := range reply.RemoteNodes {
	// 		remoteNode := &RemoteNode{
	// 			Addr:       node.Addr,
	// 			ID:         node.Id,
	// 			isLeader:   node.IsLeader,
	// 			isElection: node.IsElection,
	// 		}

	// 		if remoteNode.isLeader {
	// 			leader = remoteNode
	// 		}
	// 		if node.IsElection {
	// 			numElection++
	// 		}
	// 		ring.AddNode(remoteNode)
	// 	}
	// 	node.Ring = ring
	// }

	// if leader == nil {
	// 	fmt.Println("I am leader")
	// 	node.RemoteSelf.isLeader = true
	// 	node.RemoteSelf.isElection = true
	// 	node.Ring.AddNode(node.RemoteSelf)
	// 	// node.raft.Open(true, string(node.RemoteSelf.ID[:]))
	// 	node.Leader = NewLeader()
	// 	go node.leader()
	// } else {
	// 	if numElection < 5 {
	// 		node.RemoteSelf.isElection = true
	// 		// node.raft.Open(false, string(node.RemoteSelf.ID[:]))
	// 		// node.raft.Join(string(leader.ID[:]), leader.Addr)
	// 	}
	// }
	// go node.run()

	// return node, err
}

func (node *Node) RunNormal() {
	node.state = NormalState
	if node.RemoteSelf.isLeader {
		node.Leader = NewLeader()
		go node.leader()
	}
	go node.run()
}

func (node *Node) init() {
	node.IsShutdown = false
	node.store = NewStore()

	node.stats = new(Stats)
	node.raft = raft.New()
	node.Config = NewConfig()
	node.Discovery = NewDiscovery()
	node.Ring = NewRing()

	node.transferChan = make(chan *RemoteNode)
}

// ShutdownNode gracefully shutsdown a specified Gossip node.
func ShutdownNode(node *Node) {
	node.sdLock.Lock()
	node.IsShutdown = true
	node.sdLock.Unlock()

	node.wg.Wait()
	node.Server.GracefulStop()
	node.Listener.Close()
}
