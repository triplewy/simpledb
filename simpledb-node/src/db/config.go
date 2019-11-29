package db

// KB represents kilobyte: 1024 bytes
const KB = 1024

// MB represents Megabyte: 1024 KB
const MB = 1024 * KB

// BlockSize is size of each data block: 1 KB
const BlockSize = KB

// MemTableSize is size limit of each memtable: 16 KB
const MemTableSize = 16 * KB

// KeySize is max size for key
const KeySize = 255

// ValueSize is max size for value
const ValueSize = 4096

const filenameLength = 8
const compactThreshold = 4

const multiplier = MB

const headerSize = 32

const numWorkers = 50

const oracleSize = 10000

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
