package db

func (db *DB) Read(key string, fields []string) (values map[string][]byte, err error) {
	var entry *Entry
	err = db.ViewTxn(func(txn *Txn) error {
		entry, err = txn.Read(key)
		if err != nil {
			return err
		}
		return nil
	})
	for _, field := range fields {
		if value, ok := entry.fields[field]; ok {
			values[field] = value.data
		} else {
			values[field] = []byte{}
		}
	}
	return values, err
}

func (db *DB) Scan(key string, fields []string) (entries []*Entry, err error) {
	err = db.ViewTxn(func(txn *Txn) error {
		entries, err = txn.Scan(key, "9")
		if err != nil {
			return err
		}
		return nil
	})
	return entries, err
}

func (db *DB) Update(key string, values map[string][]byte) error {
	fields := make(map[string]*Value)
	for name, data := range values {
		fields[name] = &Value{dataType: Bytes, data: data}
	}
	err := db.UpdateTxn(func(txn *Txn) error {
		txn.Write(key, fields)
		return nil
	})
	return err
}

func (db *DB) Insert(key string, values map[string][]byte) error {
	fields := make(map[string]*Value)
	for name, data := range values {
		fields[name] = &Value{dataType: Bytes, data: data}
	}
	err := db.UpdateTxn(func(txn *Txn) error {
		txn.Write(key, fields)
		return nil
	})
	return err
}

func (db *DB) Delete(key string) error {
	err := db.UpdateTxn(func(txn *Txn) error {
		txn.Delete(key)
		return nil
	})
	return err
}
