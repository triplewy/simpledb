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
	RaftAddr   string
	HTTPAddr   string
	isLeader   bool
	isElection bool
}

// Node represents data structures stored in a gossip node.
type Node struct {
	RemoteSelf *RemoteNode /* Remote node of our self */

	Listener    net.Listener /* Node listener socket */
	Server      *grpc.Server /* RPC Server */
	serverCreds credentials.TransportCredentials
	clientCreds credentials.TransportCredentials

	IsShutdown bool           /* Is node in process of shutting down? */
	sdLock     sync.RWMutex   /* RWLock for shutdown flag */
	wg         sync.WaitGroup /* WaitGroup of concurrent goroutines to sync before exiting */

	stats       *Stats
	store       *Store
	raft        *raft.Election
	Ring        *Ring
	Leader      *Leader
	Config      *Config
	Discovery   *Discovery
	HTTPService *raft.Service
	state       uint64

	transferChan  chan *RemoteNode
	electionChan  chan *RemoteNode
	rpcErrorChan  chan error
	httpErrorChan chan error
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
	host := "localhost:"
	addr := host + node.Config.RpcPort

	node.RemoteSelf = &RemoteNode{
		ID:         HashKey(addr),
		Addr:       host + node.Config.RpcPort,
		HTTPAddr:   host + node.Config.HttpPort,
		RaftAddr:   host + node.Config.RaftPort,
		isLeader:   false,
		isElection: false,
	}
	node.Ring.AddNode(node.RemoteSelf)

	listener, err := net.Listen("tcp", ":"+node.Config.RpcPort)
	if err != nil {
		return err
	}
	node.Listener = listener

	serverCreds, err := credentials.NewServerTLSFromFile("../ssl/cert.pem", "../ssl/key.pem")
	if err != nil {
		return err
	}
	clientCreds, err := credentials.NewClientTLSFromFile("../ssl/cert.pem", "")
	if err != nil {
		return err
	}

	node.serverCreds = serverCreds
	node.clientCreds = clientCreds
	node.Server = grpc.NewServer(grpc.Creds(node.serverCreds))

	pb.RegisterSimpleDbServer(node.Server, node)

	go node.runRPC(node.Server.Serve)

	return nil
}

// RunDiscovery commences discovery phase of node
func (node *Node) RunDiscovery() {
	node.state = DiscoveryState
	node.runDiscovery()
}

// RunElection commences election phase of node
func (node *Node) RunElection() error {
	node.state = ElectionState

	var leader *RemoteNode
	for _, remote := range node.Ring.Nodes {
		if remote.isLeader {
			leader = remote
		}
	}

	if leader == nil {
		node.Ring.Nodes[0].isElection = true
		node.Ring.Nodes[0].isLeader = true
		leader = node.Ring.Nodes[0]
	}

	if leader.Addr == node.RemoteSelf.Addr {
		node.RemoteSelf.isLeader = true
		node.RemoteSelf.isElection = true

		if node.Ring.Nodes.Len() > 5 {
			for i := 0; i < 5; i++ {
				node.Ring.Nodes[i].isElection = true
			}
		} else {
			if node.Ring.Nodes.Len()%2 == 1 {
				for i := 0; i < node.Ring.Nodes.Len(); i++ {
					node.Ring.Nodes[i].isElection = true
				}
			} else {
				for i := 0; i < node.Ring.Nodes.Len()-1; i++ {
					node.Ring.Nodes[i].isElection = true
				}
			}
		}

		os.MkdirAll(node.Config.RaftDir, 0700)
		node.raft.RaftDir = node.Config.RaftDir
		node.raft.RaftBind = ":" + node.Config.RaftPort

		err := node.raft.Open(true, string(node.RemoteSelf.ID[:]))
		if err != nil {
			return err
		}

		h := raft.NewSvc(":"+node.Config.HttpPort, node.raft)
		node.HTTPService = h

		Out.Println("hraftd started successfully")

		go node.runHTTP(node.HTTPService.Start)

		type electionReply struct {
			node *RemoteNode
			err  error
		}

		electionRPC := make(chan *electionReply)

		for _, remote := range node.Ring.Nodes {
			if remote.Addr != node.RemoteSelf.Addr {
				go func(remote *RemoteNode) {
					_, err := node.HeartbeatRPC(remote)
					if err != nil {
						electionRPC <- &electionReply{
							node: remote,
							err:  err,
						}
					} else {
						electionRPC <- &electionReply{
							node: remote,
							err:  nil,
						}
					}
				}(remote)
			}
		}

		for i := 0; i < node.Ring.Nodes.Len()-1; i++ {
			reply := <-electionRPC
			if reply.err != nil {
				Error.Printf("could not send HeartbeatRPC %v\n", err)
				node.Ring.RemoveNode(reply.node)
			}
		}

		close(electionRPC)

		return nil
	}

	return nil
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
	node.electionChan = make(chan *RemoteNode)
	node.rpcErrorChan = make(chan error)
	node.httpErrorChan = make(chan error)
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

type serveRPC func(net.Listener) error

func (node *Node) runRPC(fn serveRPC) {
	err := fn(node.Listener)
	if err != nil {
		node.rpcErrorChan <- err
	}
}

type serveHTTP func() error

func (node *Node) runHTTP(fn serveHTTP) {
	err := fn()
	if err != nil {
		node.httpErrorChan <- err
	}
}
