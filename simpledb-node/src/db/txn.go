package db

// Txn is Transaction struct for Optimistic Concurrency Control.
type Txn struct {
	db *DB

	startTs  uint64
	commitTs uint64

	writeCache map[string]*KV
	readSet    map[string]uint64
}

// Read gets value for a key from the DB and updates the txn readSet
func (txn *Txn) Read(key string) (interface{}, error) {
	kv, err := txn.db.Get(key, txn.startTs)
	if err != nil {
		return nil, err
	}
	txn.readSet[key] = kv.ts
	return kv.value, nil
}

// Write updates the write cache of the txn
func (txn *Txn) Write(key string, value interface{}) {
	txn.writeCache[key] = &KV{
		key:   key,
		value: value,
	}
}

// Delete updates the write cache of the txn
func (txn *Txn) Delete(key string) {
	txn.writeCache[key] = &KV{
		key:   key,
		value: nil,
	}
}

// Range gets a range of values from a start key to an end key from the DB and updates the txn readSet
func (txn *Txn) Range(startKey, endKey string) ([]*KV, error) {
	kvs, err := txn.db.Range(startKey, endKey, txn.startTs)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		txn.readSet[kv.key] = kv.ts
	}
	return kvs, nil
}

// Commit sends the txn's read and write set to the oracle for commit
func (txn *Txn) Commit() error {
	if len(txn.writeCache) == 0 {
		return nil
	}
	return txn.db.oracle.commit(txn.readSet, txn.writeCache)
}
