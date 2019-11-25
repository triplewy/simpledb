package db

// Txn is Transaction struct for Optimistic Concurrency Control.
type Txn struct {
	db       *DB
	commitID uint64

	writeCache map[string]*KV
	readSet    map[string]uint64
}

func (txn *Txn) Read(key string) (interface{}, error) {
	kv, err := txn.db.Get(key)
	if err != nil {
		return nil, err
	}
	txn.readSet[key] = kv.commitID
	return kv.value, nil
}

func (txn *Txn) Write(key string, value interface{}) {
	txn.writeCache[key] = &KV{
		key:   key,
		value: value,
	}
}

func (txn *Txn) Delete(key string) {
	txn.writeCache[key] = &KV{
		key:   key,
		value: nil,
	}
}

func (txn *Txn) Range(startKey, endKey string) ([]*KV, error) {
	kvs, err := txn.db.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		txn.readSet[kv.key] = kv.commitID
	}
	return kvs, nil
}

func (txn *Txn) Commit() error {
	if len(txn.writeCache) == 0 {
		return nil
	}
	return txn.db.oracle.commit(txn.readSet, txn.writeCache)
}
