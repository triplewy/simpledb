package db

// Txn is Transaction struct for Optimistic Concurrency Control.
type Txn struct {
	db *DB

	startTs  uint64
	commitTs uint64

	writeCache map[string]*Entry
	readSet    map[string]uint64
}

// Read gets value for a key from the DB and updates the txn readSet
func (txn *Txn) Read(key string) (*Entry, error) {
	entry, err := txn.db.read(key, txn.startTs)
	if err != nil {
		return nil, err
	}
	txn.readSet[key] = entry.ts
	return entry, nil
}

// Write updates the write cache of the txn
func (txn *Txn) Write(key string, Fields map[string]*Value) {
	txn.writeCache[key] = &Entry{
		Key:    key,
		Fields: Fields,
	}
}

// Delete updates the write cache of the txn
func (txn *Txn) Delete(key string) {
	txn.writeCache[key] = &Entry{
		Key:    key,
		Fields: nil,
	}
}

// Scan gets a range of values from a start key to an end key from the DB and updates the txn readSet
func (txn *Txn) Scan(startKey, endKey string) ([]*Entry, error) {
	kvs, err := txn.db.scan(startKey, endKey, txn.startTs)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		txn.readSet[kv.Key] = kv.ts
	}
	return kvs, nil
}

// Commit sends the txn's read and write set to the oracle for commit
func (txn *Txn) commit() error {
	if len(txn.writeCache) == 0 {
		return nil
	}
	return txn.db.oracle.commit(txn.readSet, txn.writeCache)
}
