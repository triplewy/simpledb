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
func (txn *Txn) Read(key string) (map[string]*Value, error) {
	entry, err := txn.db.read(key, txn.startTs)
	if err != nil {
		return nil, err
	}
	txn.readSet[key] = entry.ts
	return entry.fields, nil
}

// Write updates the write cache of the txn
func (txn *Txn) Write(key string, fields map[string]*Value) {
	txn.writeCache[key] = &Entry{
		key:    key,
		fields: fields,
	}
}

// Delete updates the write cache of the txn
func (txn *Txn) Delete(key string) {
	txn.writeCache[key] = &Entry{
		key:    key,
		fields: nil,
	}
}

// Scan gets a range of values from a start key to an end key from the DB and updates the txn readSet
func (txn *Txn) Scan(startKey, endKey string) ([]*Entry, error) {
	kvs, err := txn.db.scan(startKey, endKey, txn.startTs)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		txn.readSet[kv.key] = kv.ts
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
