package db

// KB represents kilobyte: 1024 bytes
const KB = 1024

// MB represents Megabyte: 1024 KB
const MB = 1024 * KB

// BlockSize is size of each data block: 1 KB
const BlockSize = 4 * KB

// MemTableSize is size limit of each memtable: 16 KB
const MemTableSize = 16 * KB

// KeySize is max size for key
const KeySize = 255

// EntrySize is max size for entire entry
const EntrySize = KB

// MaxFields is max amount of Fields per entry
const MaxFields = 10

const timestampSize = 8

const filenameLength = 8
const compactThreshold = 4

const multiplier = MB

const headerSize = 32

const numWorkers = 50

const oracleSize = 10000

const dirPerm = 0700
const filePerm = 0600

// Supported value types
const (
	Bool uint8 = iota
	Int
	Float
	String
	Bytes
	Tombstone
)
