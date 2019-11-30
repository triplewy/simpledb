package db

func (db *DB) Read(key string, fields []string) (map[string][]byte, error) {
	var result interface{}
	err := db.ViewTxn(func(txn *Txn) error {
		value, err := txn.Read(key)
		if err != nil {
			return err
		}
		result = value
		return nil
	})
	return result, err
}

func (db *DB) Scan(key string, fields []string) map[string][]byte {
	return nil
}

func (db *DB) Update(key string, values map[string][]byte) error {
	return nil
}

func (db *DB) Insert(key string, values map[string][]byte) error {
	return nil
}

func (db *DB) Delete(key string) error {
	return nil
}
