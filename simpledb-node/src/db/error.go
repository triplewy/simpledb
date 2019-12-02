package db

import "fmt"

// ErrWriteUnexpectedBytes is error if write to file returns an unexpected amount of bytes
type ErrWriteUnexpectedBytes struct {
	file string
}

// newErrWriteUnexpectedBytes creates new ErrWriteUnexpectedBytes with specified file
func newErrWriteUnexpectedBytes(file string) *ErrWriteUnexpectedBytes {
	return &ErrWriteUnexpectedBytes{file: file}
}
func (e *ErrWriteUnexpectedBytes) Error() string {
	return "Wrote unexpected amount of bytes to " + e.file
}

type ErrReadUnexpectedBytes struct {
	file string
}

func newErrReadUnexpectedBytes(file string) *ErrReadUnexpectedBytes {
	return &ErrReadUnexpectedBytes{file: file}
}
func (e *ErrReadUnexpectedBytes) Error() string {
	return "Read unexpected amount of bytes from " + e.file
}

type ErrKeyNotFound struct{}

func newErrKeyNotFound() *ErrKeyNotFound {
	return &ErrKeyNotFound{}
}
func (e *ErrKeyNotFound) Error() string {
	return "Key not found"
}

type ErrIncorrectBlockSize struct{}

func newErrIncorrectBlockSize() *ErrIncorrectBlockSize {
	return &ErrIncorrectBlockSize{}
}

func (e *ErrIncorrectBlockSize) Error() string {
	return "Data block does not match block size"
}

type ErrIncorrectValueSize struct {
	valueType uint8
	valueSize int
}

func newErrIncorrectValueSize(valueType uint8, valueSize int) *ErrIncorrectValueSize {
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
	case Bytes:
		valueType = "Bytes"
	case Tombstone:
		valueType = "Tombstone"
	}
	return fmt.Sprintf("Value byte size %d is not appropriate for %s", e.valueSize, valueType)
}

type ErrIncompatibleValue struct {
	valueType uint8
}

func newErrIncompatibleValue(valueType uint8) *ErrIncompatibleValue {
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
	case Bytes:
		valueType = "Bytes"
	case Tombstone:
		valueType = "Tombstone"
	}
	return fmt.Sprintf("Incompatible value bytes for %s", valueType)
}

type ErrNoTypeFound struct{}

func newErrNoTypeFound() *ErrNoTypeFound {
	return &ErrNoTypeFound{}
}

func (e *ErrNoTypeFound) Error() string {
	return "No DB Type found"
}

type ErrExceedMaxKeySize struct {
	key string
}

func newErrExceedMaxKeySize(key string) *ErrExceedMaxKeySize {
	return &ErrExceedMaxKeySize{key: key}
}

func (e *ErrExceedMaxKeySize) Error() string {
	return fmt.Sprintf("Key: %v has length %d, which exceeds max key size %d", e.key, len(e.key), KeySize)
}

type ErrExceedMaxValueSize struct{}

func newErrExceedMaxValueSize() *ErrExceedMaxValueSize {
	return &ErrExceedMaxValueSize{}
}

func (e *ErrExceedMaxValueSize) Error() string {
	return fmt.Sprintf("Value size has exceeded max value size: %d", EntrySize)
}

type ErrDuplicateKey struct {
	key string
}

func newErrDuplicateKey(key string) *ErrDuplicateKey {
	return &ErrDuplicateKey{key: key}
}

func (e *ErrDuplicateKey) Error() string {
	return fmt.Sprintf("Primary key already exists: %v", e.key)
}

type ErrTxnAbort struct{}

func newErrTxnAbort() *ErrTxnAbort {
	return &ErrTxnAbort{}
}

func (e *ErrTxnAbort) Error() string {
	return fmt.Sprintf("Txn aborted due to concurrent writes to key(s) from other txns")
}

type ErrExceedMaxFields struct{}

func newErrExceedMaxFields() *ErrExceedMaxFields {
	return &ErrExceedMaxFields{}
}

func (e *ErrExceedMaxFields) Error() string {
	return fmt.Sprintf("Amount of Fields in entry exceed maximum (%d) amount of Fields", MaxFields)
}

type ErrExceedMaxEntrySize struct{}

func newErrExceedMaxEntrySize() *ErrExceedMaxEntrySize {
	return &ErrExceedMaxEntrySize{}
}

func (e *ErrExceedMaxEntrySize) Error() string {
	return fmt.Sprintf("Size of entry has exceeded maximum size of %d bytes", EntrySize)
}

type ErrDecodeEntry struct{}

func newErrDecodeEntry() *ErrDecodeEntry {
	return &ErrDecodeEntry{}
}

func (e *ErrDecodeEntry) Error() string {
	return fmt.Sprintf("Error decoding entry from file")
}

type ErrBadFormattedSST struct{}

func newErrBadFormattedSST() *ErrBadFormattedSST {
	return &ErrBadFormattedSST{}
}

func (e *ErrBadFormattedSST) Error() string {
	return fmt.Sprintf("Bad formatted SST File")
}

type ErrParseValue struct {
	value *Value
}

func newErrParseValue(value *Value) *ErrParseValue {
	return &ErrParseValue{value: value}
}

func (e *ErrParseValue) Error() string {
	return fmt.Sprintf("Error parsing value in entry: %v", e.value)
}

type ErrKeyAlreadyExists struct {
	key string
}

func newErrKeyAlreadyExists(key string) *ErrKeyAlreadyExists {
	return &ErrKeyAlreadyExists{key: key}
}

func (e *ErrKeyAlreadyExists) Error() string {
	return fmt.Sprintf("Key: %v already exists in the DB", e.key)
}
