package db

import "unicode"

// Read returns Fields from the corresponding entry from the DB
func (db *DB) Read(key string, Fields []string) (*Entry, error) {
	var result *Entry
	err := db.ViewTxn(func(txn *Txn) error {
		entry, err := txn.Read(key)
		if err != nil {
			return err
		}
		result = entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	values := make(map[string]*Value)
	for _, name := range Fields {
		if value, ok := result.Fields[name]; ok {
			values[name] = value
		} else {
			values[name] = nil
		}
	}
	result.Fields = values
	return result, nil
}

// Scan takes a key and finds all entries that are greater than or equal to that key
func (db *DB) Scan(key string, Fields []string) (result []*Entry, err error) {
	maxRunes := []rune{}
	for i := 0; i < KeySize; i++ {
		maxRunes = append(maxRunes, unicode.MaxASCII)
	}
	maxKey := string(maxRunes)
	err = db.ViewTxn(func(txn *Txn) error {
		entries, err := txn.Scan(key, maxKey)
		if err != nil {
			return err
		}
		result = entries
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, entry := range result {
		values := make(map[string]*Value)
		for _, name := range Fields {
			if value, ok := entry.Fields[name]; ok {
				values[name] = value
			} else {
				values[name] = nil
			}
		}
		entry.Fields = values
	}
	return result, err
}

// Update updates certain Fields in an entry
func (db *DB) Update(key string, values map[string][]byte) error {
	exists, err := db.checkPrimaryKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return newErrKeyNotFound()
	}
	err = db.UpdateTxn(func(txn *Txn) error {
		entry, err := txn.Read(key)
		if err != nil {
			return err
		}
		for name, data := range values {
			entry.Fields[name] = &Value{DataType: Bytes, Data: data}
		}
		txn.Write(key, entry.Fields)
		return nil
	})
	return err
}

// Insert first checks if the key exists and if not, it inserts the new entry into the DB
func (db *DB) Insert(key string, values map[string][]byte) error {
	exists, err := db.checkPrimaryKey(key)
	if err != nil {
		return err
	}
	if exists {
		return newErrKeyAlreadyExists(key)
	}
	Fields := make(map[string]*Value)
	for name, data := range values {
		Fields[name] = &Value{DataType: Bytes, Data: data}
	}
	err = db.UpdateTxn(func(txn *Txn) error {
		txn.Write(key, Fields)
		return nil
	})
	return err
}

// Delete deletes an entry from the DB. If no entry with key exists, no error is thrown
func (db *DB) Delete(key string) error {
	err := db.UpdateTxn(func(txn *Txn) error {
		txn.Delete(key)
		return nil
	})
	return err
}
