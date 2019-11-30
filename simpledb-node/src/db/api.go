package db

func (db *DB) Read(key string, fields []string) map[string][]byte {
	return nil
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
