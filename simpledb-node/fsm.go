package main

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	simpledb "github.com/triplewy/simpledb-embedded"
)

// Types of Ops
const (
	Read uint8 = iota
	Scan
	Update
	Insert
	Delete
)

type command struct {
	Op             uint8
	Key            string
	ReadAttributes []string
	WriteValues    map[string]*simpledb.Value
}

type fsmReadResponse struct {
	entry *simpledb.Entry
	err   error
}

type fsmScanResponse struct {
	entries []*simpledb.Entry
	err     error
}

type fsmGenericResponse struct {
	err error
}

type fsm store

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *fsm) Apply(log *raft.Log) interface{} {
	var c command
	err := decodeMsgPack(log.Data, c)
	if err != nil {
		panic(fmt.Sprintf("failed to decode command: %v", err))
	}
	switch c.Op {
	case Read:
		entry, err := f.db.Read(c.Key, c.ReadAttributes)
		return &fsmReadResponse{entry: entry, err: err}
	case Scan:
		entries, err := f.db.Scan(c.Key, c.ReadAttributes)
		return &fsmScanResponse{entries: entries, err: err}
	case Update:
		err := f.db.Update(c.Key, c.WriteValues)
		return &fsmGenericResponse{err: err}
	case Insert:
		err := f.db.Insert(c.Key, c.WriteValues)
		return &fsmGenericResponse{err: err}
	case Delete:
		err := f.db.Delete(c.Key)
		return &fsmGenericResponse{err: err}
	default:
		return &fsmGenericResponse{err: fmt.Errorf("unknown command: %v", c.Op)}
	}
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {

}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *fsm) Restore(rc io.ReadCloser) error {

}

type fsmSnapshot store

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f *fsm) Persist(sink raft.SnapshotSink) error {

}

// Release is invoked when we are finished with the snapshot.
func (f *fsm) Release() {}
