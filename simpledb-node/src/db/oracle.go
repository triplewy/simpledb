package db

// oracle is struct that is responsible for Optimistic Concurrency Control for ACID Txns
type oracle struct {
	ts           uint64
	reqChan      chan chan uint64
	commitChan   chan *commitReq
	commitedTxns *lru
	db           *DB
}

type commitReq struct {
	readSet   map[string]uint64
	writeSet  map[string]*Entry
	replyChan chan error
}

// newOracle creates a new oracle that keeps track of current and committed Txns
func newOracle(ts uint64, db *DB) *oracle {
	oracle := &oracle{
		ts:           ts,
		reqChan:      make(chan chan uint64),
		commitChan:   make(chan *commitReq),
		commitedTxns: newLRU(oracleSize),
		db:           db,
	}
	go oracle.run()
	return oracle
}

func (oracle *oracle) next() uint64 {
	result := oracle.ts
	oracle.ts++
	return result
}

func (oracle *oracle) requestStart() uint64 {
	replyChan := make(chan uint64, 1)
	oracle.reqChan <- replyChan
	return <-replyChan
}

func (oracle *oracle) commit(readSet map[string]uint64, writeSet map[string]*Entry) error {
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
			entries := []*Entry{}
			commitTs := oracle.next()
			for key, entry := range req.writeSet {
				oracle.commitedTxns.Insert(key, commitTs)
				entry.ts = commitTs
				entries = append(entries, entry)
			}
			req.replyChan <- oracle.db.write(entries)
		}
	}
}
