package simpledb

import "fmt"

type LSM struct {
	c0   *AVLTree
	vLog *VLog
}

func NewLSM() (*LSM, error) {
	vLog, err := NewVLog()
	if err != nil {
		return nil, err
	}
	return &LSM{
		c0:   NewAVLTree(),
		vLog: vLog,
	}, nil
}

func (lsm *LSM) Insert(key, value string) error {
	numBytes, offset, err := lsm.vLog.append([]byte(value))
	if err != nil {
		return err
	}
	err = lsm.c0.Insert([]byte(key), numBytes, offset)
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Get(key string) (string, error) {
	node, err := lsm.c0.Find([]byte(key))
	if err != nil {
		fmt.Printf("c0 find: %v\n", err)
		return "", err
	}
	result, err := lsm.vLog.read(node.offset, node.numBytes)
	if err != nil {
		fmt.Printf("vlog read: %v\n", err)
		return "", err
	}
	return string(result), nil
}
