package adapter

import (
	"errors"
	"sync"

	"github.com/unit-io/unitdb-go/internal/store"
	"github.com/unit-io/unitdb/memdb"
)

const (
	dbVersion = 1.0

	adapterName = "unitdb"
	logPostfix  = ".log"
)

// adapter represents an SSD-optimized store.
// The client closes the store while its goroutines may still use it, so the
// db is guarded and the operations of a closed adapter fail or do nothing.
type adapter struct {
	mu      sync.RWMutex
	version int
	db      *memdb.DB // The underlying database to store messages.
}

var errClosed = errors.New("unitdb adapter is closed")

// Open initializes database connection
func (a *adapter) Open(path string, size int64, reset bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.db != nil {
		return errors.New("unitdb adapter is already connected")
	}

	var err error
	// Attempt to open the database
	var opts memdb.Options
	if reset {
		opts = memdb.WithLogReset()
	}

	a.db, err = memdb.Open(opts, memdb.WithLogFilePath(path), memdb.WithBufferSize(size))
	if err != nil {
		return err
	}

	return nil
}

// Close closes the underlying database connection
func (a *adapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var err error
	if a.db != nil {
		err = a.db.Close()
		a.db = nil
		a.version = -1

	}
	return err
}

// IsOpen returns true if connection to database has been established. It does not check if
// connection is actually live.
func (a *adapter) IsOpen() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db != nil
}

// GetName returns string that adapter uses to register itself with store.
func (a *adapter) GetName() string {
	return adapterName
}

// PutMessage appends the messages to the store.
func (a *adapter) PutMessage(key uint64, payload []byte) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.db == nil {
		return errClosed
	}
	if _, err := a.db.Put(key, payload); err != nil {
		return err
	}
	return nil
}

// GetMessage performs a query and attempts to fetch message for the given key
func (a *adapter) GetMessage(key uint64) (matches []byte, err error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.db == nil {
		return nil, errClosed
	}
	matches, err = a.db.Get(key)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// DeleteMessage deletes message from memdb store.
func (a *adapter) DeleteMessage(key uint64) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.db == nil {
		return errClosed
	}
	if err := a.db.Delete(key); err != nil {
		return err
	}
	return nil
}

// Keys performs a query and attempts to fetch all keys.
func (a *adapter) Keys() []uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.db == nil {
		return nil
	}
	return a.db.Keys()
}

func init() {
	adp := &adapter{}
	store.RegisterAdapter(adapterName, adp)
}
