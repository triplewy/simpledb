package gossip

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// RemoteNode represents a Non-local node.
type RemoteNode struct {
	ID   []byte
	Addr string
}

// Node represents data structures stored in a gossip node.
type Node struct {
	ID         []byte           /* Unique Node Id */
	Listener   *net.TCPListener /* Node listener socket */
	Server     *grpc.Server     /* RPC Server */
	Addr       string           /* String of listener address */
	RemoteSelf *RemoteNode      /* Remote node of our self */

	IsShutdown bool         /* Is node in process of shutting down? */
	sdLock     sync.RWMutex /* RWLock for shutdown flag */

	dataStore map[string]string /* Local datastore for this node */
	DsLock    sync.RWMutex      /* RWLock for datastore */

	wg sync.WaitGroup /* WaitGroup of concurrent goroutines to sync before exiting */
}

// KeyLength is number of bits (i.e. m value). Assumes <= 128 and divisible by 8
const KeyLength = 16

// RPCTimeout is Timeout of RPC Calls
const RPCTimeout = 5000 * time.Millisecond

// CreateDefinedNode creates a Gossip node with a pre-defined ID (useful for testing).
func CreateDefinedNode(definedID []byte) (*Node, error) {
	node := new(Node)
	err := node.init(definedID)
	if err != nil {
		return nil, err
	}
	return node, err
}

// CreateNode creates a Gossip node with random ID based on listener address.
func CreateNode() (*Node, error) {
	node := new(Node)
	err := node.init(nil)
	if err != nil {
		return nil, err
	}
	return node, err
}

// Initailize a Chord node, start listener, RPC server, and go routines.
func (node *Node) init(definedID []byte) error {
	if KeyLength > 128 || KeyLength%8 != 0 {
		log.Fatal(fmt.Sprintf("KeyLength of %v is not supported! Must be <= 128 and divisible by 8", KeyLength))
	}

	listener, _, err := OpenTCPListener()
	if err != nil {
		return err
	}

	node.ID = HashKey(listener.Addr().String())
	if definedID != nil {
		node.ID = definedID
	}

	node.Listener = listener
	node.Addr = listener.Addr().String()
	node.IsShutdown = false
	node.dataStore = make(map[string]string)

	// Populate RemoteNode that points to self
	node.RemoteSelf = new(RemoteNode)
	node.RemoteSelf.ID = node.ID
	node.RemoteSelf.Addr = node.Addr

	node.Server = grpc.NewServer()
	// RegisterChordRPCServer(node.Server, node)
	go node.Server.Serve(node.Listener)

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
