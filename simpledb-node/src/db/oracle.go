package db

// Oracle is struct that is responsible for Optimistic Concurrency Control for ACID Txns
type Oracle struct {
	seqID        uint64
	commitChan   chan *commitReq
	commitedTxns map[string]uint64
	db           *DB
}

type commitReq struct {
	readSet   map[string]uint64
	writeSet  map[string]*KV
	replyChan chan error
}

// NewOracle creates a new oracle that keeps track of current and committed Txns
func NewOracle(seqID uint64, db *DB) *Oracle {
	oracle := &Oracle{
		seqID:        seqID,
		commitChan:   make(chan *commitReq),
		commitedTxns: make(map[string]uint64),
		db:           db,
	}
	go oracle.run()
	return oracle
}

func (oracle *Oracle) next() uint64 {
	result := oracle.seqID
	oracle.seqID++
	return result
}

func (oracle *Oracle) commit(readSet map[string]uint64, writeSet map[string]*KV) error {
	replyChan := make(chan error, 1)
	commitReq := &commitReq{
		readSet:   readSet,
		writeSet:  writeSet,
		replyChan: replyChan,
	}
	oracle.commitChan <- commitReq
	return <-replyChan
}

func (oracle *Oracle) run() {
	for {
		select {
		case req := <-oracle.commitChan:
			for key, id := range req.readSet {
				if lastCommit, ok := oracle.commitedTxns[key]; ok && lastCommit > id {
					req.replyChan <- ErrTxnAbort()
					break
				}
			}
			entries := []*KV{}
			commitID := oracle.next()
			for key, kv := range req.writeSet {
				oracle.commitedTxns[key] = commitID
				entries = append(entries, &KV{
					commitID: commitID,
					key:      kv.key,
					value:    kv.value,
				})
			}
			req.replyChan <- oracle.db.BatchPut(entries)
		}
	}
}
