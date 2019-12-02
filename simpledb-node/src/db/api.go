package db

import "unicode"

// Read returns fields from the corresponding entry from the DB
func (db *DB) Read(key string, fields []string) (map[string][]byte, error) {
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
	values := make(map[string][]byte)
	for _, field := range fields {
		if value, ok := result.fields[field]; ok {
			values[field] = value.data
		} else {
			values[field] = []byte{}
		}
	}
	return values, err
}

// Scan takes a key and finds all entries that are greater than or equal to that key
func (db *DB) Scan(key string, fields []string) (result []map[string][]byte, err error) {
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
		for _, entry := range entries {
			fields := make(map[string][]byte)
			for name, value := range entry.fields {
				fields[name] = value.data
			}
			result = append(result, fields)
		}
		return nil
	})
	return result, err
}

// Update updates certain fields in an entry
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
			entry.fields[name] = &Value{dataType: Bytes, data: data}
		}
		txn.Write(key, entry.fields)
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
	fields := make(map[string]*Value)
	for name, data := range values {
		fields[name] = &Value{dataType: Bytes, data: data}
	}
	err = db.UpdateTxn(func(txn *Txn) error {
		txn.Write(key, fields)
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
