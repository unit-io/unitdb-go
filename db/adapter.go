package adapter

import "time"

// Adapter represents a message storage contract that message storage provides
// must fulfill.
type Adapter interface {
	// Open and configure the adapter
	Open(path string, size int64, dur time.Duration) error
	// Close the adapter
	Close() error
	// IsOpen checks if the adapter is ready for use
	IsOpen() bool
	// // CheckDbVersion checks if the actual database version matches adapter version.
	// CheckDbVersion() error
	// GetName returns the name of the adapter
	GetName() string

	// Append appends message to the buffer.
	Append(delFlag bool, k uint64, data []byte) error

	// PutMessage is used to store a message.
	// it returns an error if some error was encountered during storage.
	PutMessage(blockId, key uint64, payload []byte) error

	// GetMessage performs a query and attempts to fetch message for the given blockId and key
	GetMessage(blockId, key uint64) ([]byte, error)

	// Keys performs a query and attempts to fetch all keys for given blockId.
	Keys(blockId uint64) []uint64

	// DeleteMessage is used to delete message.
	// it returns an error if some error was encountered during delete.
	DeleteMessage(blockId, key uint64) error

	// Write writes message to log file, and also release older messages from log.
	Write() error

	// Recovery loads pending messages from log file into store
	Recovery(reset bool) (map[uint64][]byte, error)
}
