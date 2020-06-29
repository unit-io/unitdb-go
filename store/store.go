package store

import (
	"errors"

	"github.com/unit-io/unitd-go/packets"
)

// Store represents a message storage contract that message storage provides
// must fulfill.
type Store interface {
	// General

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

	// Put is used to store a message, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The time resolution
	// for TTL will be in seconds. The function is executed synchronously and
	// it returns an error if some error was encountered during storage.
	Put(key uint64, payload []byte) error

	// Get performs a query and attempts to fetch last n messages where
	// n is specified by limit argument. From and until times can also be specified
	// for time-series retrieval.
	Get(key uint64) ([]byte, error)

	// Delete is used to delete entry, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The function is executed synchronously and
	// it returns an error if some error was encountered during delete.
	Delete(key uint64) error
}

var store Store

func open(path string) error {
	if store == nil {
		return errors.New("store: database adapter is missing")
	}

	if store.IsOpen() {
		return errors.New("store: connection is already opened")
	}

	return store.Open(path)
}

// Open initializes the persistence system. Adapter holds a connection pool for a database instance.
// 	 name - name of the adapter requested in the config file
//   path - configuration string
func Open(path string) error {
	if err := open(path); err != nil {
		return err
	}

	return nil
}

// Close terminates connection to persistent storage.
func Close() error {
	if store.IsOpen() {
		return store.Close()
	}

	return nil
}

// IsOpen checks if persistent storage connection has been initialized.
func IsOpen() bool {
	if store != nil {
		return store.IsOpen()
	}

	return false
}

// GetAdapterName returns the name of the current adater.
func GetAdapterName() string {
	if store != nil {
		return store.GetName()
	}

	return ""
}

// InitDb open the db connection. If path is nil it will assume that the connection is already open.
// If it's non-nil, it will use the config string to open the DB connection first.
func InitDb(path string, reset bool) error {
	if !IsOpen() {
		if err := open(path); err != nil {
			return err
		}
	}
	panic("store: Init DB error")
}

// RegisterAdapter makes a persistence adapter available.
// If Register is called twice or if the adapter is nil, it panics.
func RegisterAdapter(name string, s Store) {
	if s == nil {
		panic("store: Register adapter is nil")
	}

	if store != nil {
		panic("store: adapter '" + store.GetName() + "' is already registered")
	}

	store = s
}

// MessageStore is a Message struct to hold methods for persistence mapping for the Message object.
type MessageStore struct{}

// Message is the anchor for storing/retrieving Message objects
var Message MessageStore

// handle which outgoing messages are stored
func (m *MessageStore) PersistOutbound(key uint64, msg packets.Packet) {
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Pubcomp:
			// Sending puback. delete matching publish
			// from ibound
			store.Delete(key)
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel, *packets.Subscribe, *packets.Unsubscribe:
			// Sending publish. store in obound
			// until puback received
			store.Put(key, msg.Encode())
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Sending publish. store in obound
			// until pubrel received
			store.Put(key, msg.Encode())
		default:
		}
	}
}

// handle which incoming messages are stored
func (m *MessageStore) PersistInbound(key uint64, msg packets.Packet) {
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Suback, *packets.Unsuback, *packets.Pubcomp:
			// Received a puback. delete matching publish
			// from obound
			store.Delete(key)
		case *packets.Publish, *packets.Pubrec, *packets.Pingresp, *packets.Connack:
		default:
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel:
			// Received a publish. store it in ibound
			// until puback sent
			store.Put(key, msg.Encode())
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Received a publish. store it in ibound
			// until pubrel received
			store.Put(key, msg.Encode())
		default:
		}
	}
}
