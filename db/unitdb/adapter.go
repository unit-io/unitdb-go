package adapter

import (
	"errors"
	"math/rand"
	"os"

	"github.com/unit-io/unitd-go/store"
	"github.com/unit-io/unitdb/memdb"
)

const (
	defaultDatabase = "unitd"

	dbVersion = 1.0

	adapterName = "unitdb"
)

const (
	// Maximum number of records to return
	maxResults = 1024
	// Maximum TTL for message
	maxTTL = "24h"
)

// adapter represents an SSD-optimized store.
type adapter struct {
	cacheID uint64
	db      *memdb.DB // The underlying database to store messages.
	version int
}

// Open initializes database connection
func (a *adapter) Open(path string) error {
	if a.db != nil {
		return errors.New("unitdb adapter is already connected")
	}

	var err error
	// Make sure we have a directory
	if err := os.MkdirAll(path, 0777); err != nil {
		return errors.New("adapter.Open, Unable to create db dir")
	}

	// Attempt to open the database
	a.db, err = memdb.Open(1 << 33)
	if err != nil {
		return err
	}
	// memdb blockcache id
	a.cacheID = uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
	return nil
}

// Close closes the underlying database connection
func (a *adapter) Close() error {
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
	return a.db != nil
}

// GetName returns string that adapter uses to register itself with store.
func (a *adapter) GetName() string {
	return adapterName
}

// Put appends the messages to the store.
func (a *adapter) Put(key uint64, payload []byte) error {
	blockId := a.cacheID ^ key
	if err := a.db.Set(blockId, key, payload); err != nil {
		return err
	}
	return nil
}

// Get performs a query and attempts to fetch last n messages where
// n is specified by limit argument. From and until times can also be specified
// for time-series retrieval.
func (a *adapter) Get(key uint64) (matches []byte, err error) {
	blockId := a.cacheID ^ key
	matches, err = a.db.Get(blockId, key)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// Put appends the messages to the store.
func (a *adapter) Delete(key uint64) error {
	blockId := a.cacheID ^ key
	if err := a.db.Remove(blockId, key); err != nil {
		return err
	}
	return nil
}

func init() {
	store.RegisterAdapter(adapterName, &adapter{})
}
