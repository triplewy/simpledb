package simpledb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Nodes []RemoteNode `json:"nodes,omitempty"`
}

// Election is struct for Raft Protocol and cluster membership
type Election struct {
	raft     *raft.Raft
	RaftDir  string
	RaftBind string

	nLock sync.Mutex
	nodes []RemoteNode

	logger *log.Logger
}

// New returns a new Store.
func New(raftDir, raftBind string) *Election {
	return &Election{
		nodes:    []RemoteNode{},
		RaftDir:  raftDir,
		RaftBind: raftBind,
		logger:   log.New(os.Stderr, "[election] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (node *Election) Open(joinAddr string) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	localID := HashKey(joinAddr)
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", node.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(node.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(node.RaftDir, 1, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	// Instantiate the Raft systemnode.
	fsm := &fsm{nodes: node.nodes}

	ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	node.raft = ra

	if joinAddr == "" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
		node.Set([]RemoteNode{RemoteNode{ID: localID, Addr: node.RaftBind}})
	} else {
		node.Join(joinAddr)
	}

	return nil
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (node *Election) Join(addr string) error {
	nodeID := HashKey(addr)

	Out.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := node.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		Out.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				Out.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := node.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
			// TODO: Remove node from remotenNodes
		}
	}

	f := node.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	Out.Printf("node %s at %s joined successfully", nodeID, addr)
	node.nodes = append(node.nodes, RemoteNode{ID: nodeID, Addr: addr})
	node.Set(node.nodes)
	return nil
}

type fsm struct {
	nLock sync.Mutex
	nodes []RemoteNode
}

// Get returns the value for the given key.
func (node *Election) Get() ([]RemoteNode, error) {
	node.nLock.Lock()
	defer node.nLock.Unlock()
	return node.nodes, nil
}

// Set sets the value for the given key.
func (node *Election) Set(nodes []RemoteNode) error {
	if node.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Nodes: nodes,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := node.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	f.nLock.Lock()
	defer f.nLock.Unlock()
	f.nodes = c.Nodes
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.nLock.Lock()
	defer f.nLock.Unlock()

	copy := []RemoteNode{}
	copy = append(f.nodes[:0:0], f.nodes...)

	return &fsmSnapshot{nodes: copy}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	nodes := []RemoteNode{}
	if err := json.NewDecoder(rc).Decode(&nodes); err != nil {
		return err
	}

	f.nodes = nodes
	return nil
}

type fsmSnapshot struct {
	nodes []RemoteNode
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.nodes)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
