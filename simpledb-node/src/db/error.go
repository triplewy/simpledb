package db

import "fmt"

type errWriteUnexpectedBytes struct {
	file string
}

func ErrWriteUnexpectedBytes(file string) *errWriteUnexpectedBytes {
	return &errWriteUnexpectedBytes{file: file}
}
func (e *errWriteUnexpectedBytes) Error() string {
	return "Wrote unexpected amount of bytes to " + e.file
}

type errReadUnexpectedBytes struct {
	file string
}

func ErrReadUnexpectedBytes(file string) *errReadUnexpectedBytes {
	return &errReadUnexpectedBytes{file: file}
}
func (e *errReadUnexpectedBytes) Error() string {
	return "Read unexpected amount of bytes from " + e.file
}

type errKeyNotFound struct{}

func ErrKeyNotFound() *errKeyNotFound {
	return &errKeyNotFound{}
}
func (e *errKeyNotFound) Error() string {
	return "Key not found"
}

type errIncorrectBlockSize struct{}

func ErrIncorrectBlockSize() *errIncorrectBlockSize {
	return &errIncorrectBlockSize{}
}

func (e *errIncorrectBlockSize) Error() string {
	return "Data block does not match block size"
}

type errIncorrectValueSize struct {
	valueType uint8
	valueSize int
}

func ErrIncorrectValueSize(valueType uint8, valueSize int) *errIncorrectValueSize {
	return &errIncorrectValueSize{
		valueType: valueType,
		valueSize: valueSize,
	}
}

func (e *errIncorrectValueSize) Error() string {
	valueType := ""
	switch e.valueType {
	case Bool:
		valueType = "Bool"
	case Int:
		valueType = "Int"
	case Float:
		valueType = "Float"
	case String:
		valueType = "String"
	case Tombstone:
		valueType = "Tombstone"
	}
	return fmt.Sprintf("Value byte size %d is not appropriate for %s", e.valueSize, valueType)
}

type errIncompatibleValue struct {
	valueType uint8
}

func ErrIncompatibleValue(valueType uint8) *errIncompatibleValue {
	return &errIncompatibleValue{
		valueType: valueType,
	}
}

func (e *errIncompatibleValue) Error() string {
	valueType := ""
	switch e.valueType {
	case Bool:
		valueType = "Bool"
	case Int:
		valueType = "Int"
	case Float:
		valueType = "Float"
	case String:
		valueType = "String"
	case Tombstone:
		valueType = "Tombstone"
	}
	return fmt.Sprintf("Incompatible value bytes for %s", valueType)
}

type errNoTypeFound struct{}

func ErrNoTypeFound() *errNoTypeFound {
	return &errNoTypeFound{}
}

func (e *errNoTypeFound) Error() string {
	return "No DB Type found"
}

type errExceedMaxKeySize struct {
	key string
}

func ErrExceedMaxKeySize(key string) *errExceedMaxKeySize {
	return &errExceedMaxKeySize{key: key}
}

func (e *errExceedMaxKeySize) Error() string {
	return fmt.Sprintf("Key: %v has length %d, which exceeds max key size %d", e.key, len(e.key), KeySize)
}

type errExceedMaxValueSize struct{}

func ErrExceedMaxValueSize() *errExceedMaxValueSize {
	return &errExceedMaxValueSize{}
}

func (e *errExceedMaxValueSize) Error() string {
	return fmt.Sprintf("Value size has exceeded max value size: %d", ValueSize)
}

type errDuplicateKey struct {
	key string
}

func ErrDuplicateKey(key string) *errDuplicateKey {
	return &errDuplicateKey{key: key}
}

func (e *errDuplicateKey) Error() string {
	return fmt.Sprintf("Primary key already exists: %v", e.key)
}

type errTxnAbort struct{}

func ErrTxnAbort() *errTxnAbort {
	return &errTxnAbort{}
}

func (e *errTxnAbort) Error() string {
	return fmt.Sprintf("Txn aborted due to concurrent writes to key(s) from other txns")
}
