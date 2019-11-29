package db

// Oracle is struct that is responsible for Optimistic Concurrency Control for ACID Txns
type Oracle struct {
	timestamp    uint64
	reqChan      chan chan uint64
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
func NewOracle(timestamp uint64, db *DB) *Oracle {
	oracle := &Oracle{
		timestamp:    timestamp,
		reqChan:      make(chan chan uint64),
		commitChan:   make(chan *commitReq),
		commitedTxns: make(map[string]uint64),
		db:           db,
	}
	go oracle.run()
	return oracle
}

func (oracle *Oracle) next() uint64 {
	result := oracle.timestamp
	oracle.timestamp++
	return result
}

func (oracle *Oracle) requestStart() uint64 {
	replyChan := make(chan uint64, 1)
	oracle.reqChan <- replyChan
	return <-replyChan
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
	SelectStatement:
		select {
		case replyChan := <-oracle.reqChan:
			startID := oracle.next()
			replyChan <- startID
		case req := <-oracle.commitChan:
			for key, ts := range req.readSet {
				if lastCommit, ok := oracle.commitedTxns[key]; ok && lastCommit > ts {
					req.replyChan <- NewErrTxnAbort()
					break SelectStatement
				}
			}
			entries := []*KV{}
			commitTs := oracle.next()
			for key, kv := range req.writeSet {
				oracle.commitedTxns[key] = commitTs
				entries = append(entries, &KV{
					ts:    commitTs,
					key:   kv.key,
					value: kv.value,
				})
			}
			req.replyChan <- oracle.db.BatchPut(entries)
		}
	}
}
