package main

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	simpledb "github.com/triplewy/simpledb-embedded"
)

// Types of Ops
const (
	Write uint8 = iota
	Delete
)

// Command is placed in logs for snapshot purposes
type Command struct {
	Op     uint8
	Key    string
	Values map[string]*simpledb.Value
}

type fsmResponse struct {
	err error
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (store *store) Apply(log *raft.Log) interface{} {
	var c Command
	err := decodeMsgPack(log.Data, &c)
	if err != nil {
		panic(fmt.Sprintf("failed to decode command: %v", err))
	}
	switch c.Op {
	case Write:
		err := store.db.Insert(c.Key, c.Values)
		return &fsmResponse{err: err}
	case Delete:
		err := store.db.Delete(c.Key)
		return &fsmResponse{err: err}
	default:
		return &fsmResponse{err: fmt.Errorf("unknown command: %v", c.Op)}
	}
}

type fsmSnapshot struct {
	logs []*raft.Log
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (store *store) Snapshot() (raft.FSMSnapshot, error) {
	logs, err := store.RangeLogs()
	return &fsmSnapshot{logs: logs}, err
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (store *store) Restore(rc io.ReadCloser) error {
	var err error
	sizeBuf := make([]byte, 8)
	_, err = rc.Read(sizeBuf)
	for err == nil {
		size := bytesToUint64(sizeBuf)
		buf := make([]byte, size)
		_, err := rc.Read(buf)
		if err != nil {
			return err
		}
		var log raft.Log
		err = decodeMsgPack(buf, &log)
		if err != nil {
			return err
		}
		var c Command
		err = decodeMsgPack(log.Data, &c)
		if err != nil {
			return err
		}
		switch c.Op {
		case Write:
			err := store.db.Insert(c.Key, c.Values)
			if err != nil {
				return err
			}
		case Delete:
			err := store.db.Delete(c.Key)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown command: %v", c.Op)
		}
		_, err = rc.Read(sizeBuf)
	}
	return nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		for _, log := range f.logs {
			buf, err := encodeMsgPack(log)
			if err != nil {
				return err
			}
			size := uint64ToBytes(uint64(len(buf.Bytes())))
			if _, err := sink.Write(append(size, buf.Bytes()...)); err != nil {
				return err
			}
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

// Release is invoked when we are finished with the snapshot.
func (f *fsmSnapshot) Release() {}
