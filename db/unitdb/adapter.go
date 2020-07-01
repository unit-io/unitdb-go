package adapter

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/unit-io/unitd-go/store"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/wal"
)

const (
	defaultDatabase = "unitd"

	dbVersion = 1.0

	adapterName = "unitdb"
	logPostfix  = ".log"
)

const (
	// Maximum number of records to return
	maxResults = 1024
	// Maximum TTL for message
	maxTTL = "24h"
)

// adapter represents an SSD-optimized store.
type adapter struct {
	db        *memdb.DB // The underlying database to store messages.
	logWriter *wal.Writer
	wal       *wal.WAL
	version   int

	// close
	closer io.Closer
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
	return nil
}

// Close closes the underlying database connection
func (a *adapter) Close() error {
	var err error
	if a.db != nil {
		err = a.db.Close()
		a.db = nil
		a.version = -1

		var err error
		if a.closer != nil {
			if err1 := a.closer.Close(); err == nil {
				err = err1
			}
			a.closer = nil
		}
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
func (a *adapter) Put(blockId, key uint64, payload []byte) error {
	if err := a.db.Set(blockId, key, payload); err != nil {
		return err
	}
	return nil
}

// Get performs a query and attempts to fetch message for the given blockId and key
func (a *adapter) Get(blockId, key uint64) (matches []byte, err error) {
	matches, err = a.db.Get(blockId, key)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// Keys performs a query and attempts to fetch all keys for given blockId.
func (a *adapter) Keys(blockId uint64) []uint64 {
	return a.db.Keys(blockId)
}

// Put appends the messages to the store.
func (a *adapter) Delete(blockId, key uint64) error {
	if err := a.db.Remove(blockId, key); err != nil {
		return err
	}
	return nil
}

// NewWriter creates new log writer.
func (a *adapter) NewWriter() error {
	if w, err := a.wal.NewWriter(); err == nil {
		a.logWriter = w
		return err
	}
	return nil
}

// Append appends messages to the log.
func (a *adapter) Append(data []byte) <-chan error {
	return a.logWriter.Append(data)
}

// SignalInitWrite signals to write log.
func (a *adapter) SignalInitWrite(seq uint64) <-chan error {
	return a.logWriter.SignalInitWrite(seq)
}

// SignalLogApplied signals log has been applied for given upper sequence.
// logs are released from wal so that space can be reused.
func (a *adapter) SignalLogApplied(seq uint64) error {
	return a.wal.SignalLogApplied(seq)
}

// Recovery recovers pending messages from log file.
func (a *adapter) Recovery(path string, size int64, reset bool) (map[uint64][]byte, error) {
	m := make(map[uint64][]byte) // map[key]msg
	logOpts := wal.Options{Path: path + logPostfix, TargetSize: size, BufferSize: size}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		return m, err
	}

	a.closer = wal
	a.wal = wal
	if !needLogRecovery || reset {
		return m, nil
	}

	// start log recovery
	r, err := wal.NewReader()
	if err != nil {
		return m, err
	}
	err = r.Read(func(lSeq uint64, last bool) (ok bool, err error) {
		l := r.Count()
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			dBit := logData[0]
			key := binary.LittleEndian.Uint64(logData[1:9])
			msg := logData[9:]
			if dBit == 1 {
				if _, exists := m[key]; exists {
					delete(m, key)
				}
			}
			m[key] = msg
		}
		return false, nil
	})
	// if err !=nil{
	// 	return err
	// }

	// for k, msg := range m {
	// 	blockId :=
	// 	a.db.Set()
	// }
	return m, err
}

func init() {
	store.RegisterAdapter(adapterName, &adapter{})
}
