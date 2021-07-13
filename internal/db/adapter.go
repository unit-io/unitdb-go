package adapter

// Adapter represents a message storage contract that message storage provides
// must fulfill.
type Adapter interface {
	// Open and configure the adapter
	Open(path string, size int64, reset bool) error
	// Close the adapter
	Close() error
	// IsOpen checks if the adapter is ready for use
	IsOpen() bool
	// // CheckDbVersion checks if the actual database version matches adapter version.
	// CheckDbVersion() error
	// GetName returns the name of the adapter
	GetName() string

	// PutMessage is used to store a message.
	// it returns an error if some error was encountered during storage.
	PutMessage(key uint64, payload []byte) error

	// GetMessage performs a query and attempts to fetch message for the given key
	GetMessage(key uint64) ([]byte, error)

	// DeleteMessage is used to delete message.
	// it returns an error if some error was encountered during delete.
	DeleteMessage(key uint64) error

	// Keys performs a query and attempts to fetch all keys.
	Keys() []uint64
}
