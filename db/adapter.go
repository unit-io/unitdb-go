package adapter

// Log represents a message storage contract that message storage provides
// must fulfill.
type Log interface {
	// Open and configure the adapter
	Open(path string) error
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
	PutMessage(blockId, key uint64, payload []byte) error

	// GetMessage performs a query and attempts to fetch message for the given blockId and key
	GetMessage(blockId, key uint64) ([]byte, error)

	// Keys performs a query and attempts to fetch all keys for given blockId.
	Keys(blockId uint64) []uint64

	// DeleteMessage is used to delete message.
	// it returns an error if some error was encountered during delete.
	DeleteMessage(blockId, key uint64) error

	// NewWriter creates new log writer
	NewWriter() error

	// Append appends messages to the log
	Append(data []byte) <-chan error

	// SignalInitWrite signal to write messages to log file
	SignalInitWrite(seq uint64) <-chan error

	// SignalLogApplied signals log has been applied for given upper sequence.
	// logs are released from wal so that space can be reused.
	SignalLogApplied(seq uint64) error

	// Recovery loads pending messages from log file into store
	Recovery(path string, size int64, reset bool) (map[uint64][]byte, error)
}
