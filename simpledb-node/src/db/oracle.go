package db

// oracle is struct that is responsible for Optimistic Concurrency Control for ACID Txns
type oracle struct {
	timestamp    uint64
	reqChan      chan chan uint64
	commitChan   chan *commitReq
	commitedTxns *lru
	db           *DB
}

type commitReq struct {
	readSet   map[string]uint64
	writeSet  map[string]*kv
	replyChan chan error
}

// newOracle creates a new oracle that keeps track of current and committed Txns
func newOracle(timestamp uint64, db *DB) *oracle {
	oracle := &oracle{
		timestamp:    timestamp,
		reqChan:      make(chan chan uint64),
		commitChan:   make(chan *commitReq),
		commitedTxns: newLRU(oracleSize),
		db:           db,
	}
	go oracle.run()
	return oracle
}

func (oracle *oracle) next() uint64 {
	result := oracle.timestamp
	oracle.timestamp++
	return result
}

func (oracle *oracle) requestStart() uint64 {
	replyChan := make(chan uint64, 1)
	oracle.reqChan <- replyChan
	return <-replyChan
}

func (oracle *oracle) commit(readSet map[string]uint64, writeSet map[string]*kv) error {
	replyChan := make(chan error, 1)
	commitReq := &commitReq{
		readSet:   readSet,
		writeSet:  writeSet,
		replyChan: replyChan,
	}
	oracle.commitChan <- commitReq
	return <-replyChan
}

func (oracle *oracle) run() {
	for {
	SelectStatement:
		select {
		case replyChan := <-oracle.reqChan:
			startID := oracle.next()
			replyChan <- startID
		case req := <-oracle.commitChan:
			for key, ts := range req.readSet {
				if lastCommit, ok := oracle.commitedTxns.Get(key); ok && lastCommit > ts {
					req.replyChan <- newErrTxnAbort()
					break SelectStatement
				}
				if oracle.commitedTxns.maxTs > ts {
					req.replyChan <- newErrTxnAbort()
					break SelectStatement
				}
			}
			entries := []*kv{}
			commitTs := oracle.next()
			for key, item := range req.writeSet {
				oracle.commitedTxns.Insert(key, commitTs)
				entries = append(entries, &kv{
					ts:    commitTs,
					key:   item.key,
					value: item.value,
				})
			}
			req.replyChan <- oracle.db.batchPut(entries)
		}
	}
}
