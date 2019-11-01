package db

// BlockSize is size of each data block
const BlockSize = 4 * 1024

const MemTableSize = 2 * 1024 * 1024

// KeySize is max size for key
const KeySize = 255

// ValueSize is max size for value
const ValueSize = 4096

const filenameLength = 8
const compactThreshold = 4

const multiplier = 10240

const gcThreshold = 4 * 1024 * 1024

const headerSize = 32

// EntrySizeConstant represents size of SeqID + Type + KeySize + ValueSize,
// which all have constant byte sizes in an LSM Data Entry
const EntrySizeConstant = 12

// Supported value types
const (
	Bool uint8 = iota
	Int
	Float
	String
	Tombstone
)
