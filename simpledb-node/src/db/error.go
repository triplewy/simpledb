package db

import "fmt"

// ErrWriteUnexpectedBytes is error if write to file returns an unexpected amount of bytes
type ErrWriteUnexpectedBytes struct {
	file string
}

// NewErrWriteUnexpectedBytes creates new ErrWriteUnexpectedBytes with specified file
func NewErrWriteUnexpectedBytes(file string) *ErrWriteUnexpectedBytes {
	return &ErrWriteUnexpectedBytes{file: file}
}
func (e *ErrWriteUnexpectedBytes) Error() string {
	return "Wrote unexpected amount of bytes to " + e.file
}

type ErrReadUnexpectedBytes struct {
	file string
}

func NewErrReadUnexpectedBytes(file string) *ErrReadUnexpectedBytes {
	return &ErrReadUnexpectedBytes{file: file}
}
func (e *ErrReadUnexpectedBytes) Error() string {
	return "Read unexpected amount of bytes from " + e.file
}

type ErrKeyNotFound struct{}

func NewErrKeyNotFound() *ErrKeyNotFound {
	return &ErrKeyNotFound{}
}
func (e *ErrKeyNotFound) Error() string {
	return "Key not found"
}

type ErrIncorrectBlockSize struct{}

func NewErrIncorrectBlockSize() *ErrIncorrectBlockSize {
	return &ErrIncorrectBlockSize{}
}

func (e *ErrIncorrectBlockSize) Error() string {
	return "Data block does not match block size"
}

type ErrIncorrectValueSize struct {
	valueType uint8
	valueSize int
}

func NewErrIncorrectValueSize(valueType uint8, valueSize int) *ErrIncorrectValueSize {
	return &ErrIncorrectValueSize{
		valueType: valueType,
		valueSize: valueSize,
	}
}

func (e *ErrIncorrectValueSize) Error() string {
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

type ErrIncompatibleValue struct {
	valueType uint8
}

func NewErrIncompatibleValue(valueType uint8) *ErrIncompatibleValue {
	return &ErrIncompatibleValue{
		valueType: valueType,
	}
}

func (e *ErrIncompatibleValue) Error() string {
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

type ErrNoTypeFound struct{}

func NewErrNoTypeFound() *ErrNoTypeFound {
	return &ErrNoTypeFound{}
}

func (e *ErrNoTypeFound) Error() string {
	return "No DB Type found"
}

type ErrExceedMaxKeySize struct {
	key string
}

func NewErrExceedMaxKeySize(key string) *ErrExceedMaxKeySize {
	return &ErrExceedMaxKeySize{key: key}
}

func (e *ErrExceedMaxKeySize) Error() string {
	return fmt.Sprintf("Key: %v has length %d, which exceeds max key size %d", e.key, len(e.key), KeySize)
}

type ErrExceedMaxValueSize struct{}

func NewErrExceedMaxValueSize() *ErrExceedMaxValueSize {
	return &ErrExceedMaxValueSize{}
}

func (e *ErrExceedMaxValueSize) Error() string {
	return fmt.Sprintf("Value size has exceeded max value size: %d", ValueSize)
}

type ErrDuplicateKey struct {
	key string
}

func NewErrDuplicateKey(key string) *ErrDuplicateKey {
	return &ErrDuplicateKey{key: key}
}

func (e *ErrDuplicateKey) Error() string {
	return fmt.Sprintf("Primary key already exists: %v", e.key)
}

type ErrTxnAbort struct{}

func NewErrTxnAbort() *ErrTxnAbort {
	return &ErrTxnAbort{}
}

func (e *ErrTxnAbort) Error() string {
	return fmt.Sprintf("Txn aborted due to concurrent writes to key(s) from other txns")
}
