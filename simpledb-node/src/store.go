package simpledb

import (
	"bytes"
	"errors"
	"sync"
)

type Store struct {
	store        map[string]string /* Local datastore for this node */
	sync.RWMutex                   /* RWLock for datastore */
}

type KV struct {
	key   string
	value string
}

func NewStore() *Store {
	return &Store{
		store: make(map[string]string),
	}
}

func (store *Store) get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()
	if val, ok := store.store[key]; ok {
		return val, nil
	}
	return "", errors.New("Key does not exist")
}

func (store *Store) set(key, value string) {
	store.Lock()
	defer store.Unlock()
	store.store[key] = value
}

func (store *Store) del(key string) {
	store.Lock()
	defer store.Unlock()
	delete(store.store, key)
}

func (store *Store) transfer(id []byte) []*KV {
	result := []*KV{}
	for k, v := range store.store {
		if bytes.Compare(HashKey(k), id) <= 0 {
			result = append(result, &KV{key: k, value: v})
		}
	}
	return result
}
