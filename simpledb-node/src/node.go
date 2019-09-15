package simpledb

import (
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/triplewy/simpledb/grpc"
)

// RemoteNode represents a Non-local node.
type RemoteNode struct {
	ID   []byte
	Addr string
}

// Node represents data structures stored in a gossip node.
type Node struct {
	ID         []byte      /* Unique Node Id */
	Addr       string      /* String of listener address */
	RemoteSelf *RemoteNode /* Remote node of our self */

	Listener net.Listener /* Node listener socket */
	Server   *grpc.Server /* RPC Server */
	creds    credentials.TransportCredentials
	grpcPort int

	stats *Stats

	IsShutdown bool         /* Is node in process of shutting down? */
	sdLock     sync.RWMutex /* RWLock for shutdown flag */

	dataStore map[string]string /* Local datastore for this node */
	DsLock    sync.RWMutex      /* RWLock for datastore */

	receiveChan chan string
	wg          sync.WaitGroup /* WaitGroup of concurrent goroutines to sync before exiting */

	isElection bool
	raft       *Election

	nodes []RemoteNode
}

// KeyLength is number of bits (i.e. m value). Assumes <= 128 and divisible by 8
const KeyLength = 16

// CreateNode creates a Gossip node with random ID based on listener address.
func CreateNode() (*Node, error) {
	node := new(Node)
	err := node.init()
	if err != nil {
		return nil, err
	}
	return node, err
}

// Initailize a Chord node, start listener, RPC server, and go routines.
func (node *Node) init() error {
	node.grpcPort = 50051

	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(node.grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	node.ID = HashKey(listener.Addr().String())

	node.Listener = listener
	node.Addr = listener.Addr().String()
	node.IsShutdown = false
	node.dataStore = make(map[string]string)

	// Populate RemoteNode that points to self
	node.RemoteSelf = new(RemoteNode)
	node.RemoteSelf.ID = node.ID
	node.RemoteSelf.Addr = node.Addr

	creds, err := credentials.NewServerTLSFromFile("../ssl/cert.pem", "../ssl/key.pem")
	if err != nil {
		log.Fatalf("could not create credentials: %v", err)
	}

	node.creds = creds
	node.Server = grpc.NewServer(grpc.Creds(node.creds))

	pb.RegisterSimpleDbServer(node.Server, node)

	node.stats = new(Stats)

	go node.run()

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

func (node *Node) run() {
	ticker := time.NewTicker(5 * time.Second)
	heartbeat := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-ticker.C:
			go node.getStats()
		case <-heartbeat.C:

		}
	}
}
