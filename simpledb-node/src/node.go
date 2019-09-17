package simpledb

import (
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/triplewy/simpledb/grpc"
)

// GrpcPort is port used for gRPC
var GrpcPort = ":50051"

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

	Listener net.Listener /* Node listener socket */
	Server   *grpc.Server /* RPC Server */

	serverCreds credentials.TransportCredentials
	clientCreds credentials.TransportCredentials

	stats *Stats

	IsShutdown bool         /* Is node in process of shutting down? */
	sdLock     sync.RWMutex /* RWLock for shutdown flag */

	dataStore map[string]string /* Local datastore for this node */
	DsLock    sync.RWMutex      /* RWLock for datastore */

	receiveChan chan string
	wg          sync.WaitGroup /* WaitGroup of concurrent goroutines to sync before exiting */

	raft *Election

	Ring *Ring

	Leader *Leader
}

// CreateNode creates a Gossip node with random ID based on listener address.
func CreateNode(joinAddr string) (*Node, error) {
	node := new(Node)
	err := node.init()
	if err != nil {
		return nil, err
	}

	var leader *RemoteNode
	numElection := 0
	if joinAddr != "" {
		reply, err := node.JoinNodesRPC(&RemoteNode{Addr: joinAddr, ID: HashKey(joinAddr)})
		if err != nil {
			log.Fatalf("failed to join node %v", err)
		}
		ring := new(Ring)
		for _, node := range reply.RemoteNodes {
			fmt.Println(node.IsLeader)
			remoteNode := &RemoteNode{
				Addr:       node.Addr,
				ID:         node.Id,
				isLeader:   node.IsLeader,
				isElection: node.IsElection,
			}

			if remoteNode.isLeader {
				leader = remoteNode
			}
			if node.IsElection {
				numElection++
			}
			ring.AddNode(remoteNode)
		}
		node.Ring = ring
	}

	if leader == nil {
		fmt.Println("I am leader")
		node.RemoteSelf.isLeader = true
		node.RemoteSelf.isElection = true
		node.Ring.RemoveNode(node.RemoteSelf)
		node.Ring.AddNode(node.RemoteSelf)
		// node.raft.Open(true, string(node.RemoteSelf.ID[:]))
		node.Leader = NewLeader()
		go node.leader()
	} else {
		if numElection < 5 {
			node.RemoteSelf.isElection = true
			// node.raft.Open(false, string(node.RemoteSelf.ID[:]))
			// node.raft.Join(string(leader.ID[:]), leader.Addr)
			go node.follower()
		}
	}
	go node.runGetStats()

	return node, err
}

func (node *Node) init() error {
	listener, err := net.Listen("tcp", GrpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	node.Listener = listener
	// If on Brown network
	// node.Addr = GetOutboundIP().String
	// If on local computer
	node.Addr = "localhost" + GrpcPort
	node.ID = HashKey(node.Addr)
	node.IsShutdown = false
	node.dataStore = make(map[string]string)

	// Populate RemoteNode that points to self
	node.RemoteSelf = &RemoteNode{
		Addr:       node.Addr,
		ID:         node.ID,
		isLeader:   false,
		isElection: false,
	}

	node.Ring = NewRing()
	node.Ring.AddNode(node.RemoteSelf)

	serverCreds, err := credentials.NewServerTLSFromFile("../ssl/cert.pem", "../ssl/key.pem")
	if err != nil {
		log.Fatalf("could not create credentials: %v", err)
	}
	clientCreds, err := credentials.NewClientTLSFromFile("../ssl/cert.pem", "")
	if err != nil {
		log.Fatalf("could not create credentials: %v", err)
	}

	node.serverCreds = serverCreds
	node.clientCreds = clientCreds
	node.Server = grpc.NewServer(grpc.Creds(node.serverCreds))

	pb.RegisterSimpleDbServer(node.Server, node)

	node.stats = new(Stats)

	// node.raft = Election.New()
	// node.raft.RaftBind =
	// node.raft.RaftDir =

	return err
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
