package store

import (
	"bytes"
	"errors"
	"fmt"

	adapter "github.com/unit-io/unitdb-go/db"
	"github.com/unit-io/unitdb-go/packets"
)

var adp adapter.Adapter

func open(path string, size int64, reset bool) error {
	if adp == nil {
		return errors.New("store: database adapter is missing")
	}

	if adp.IsOpen() {
		return errors.New("store: connection is already opened")
	}

	return adp.Open(path, size, reset)
}

// Open initializes the persistence. Adapter holds a connection pool for a database instance.
//   path - database path
func Open(path string, size int64, reset bool) error {
	if err := open(path, size, reset); err != nil {
		return err
	}

	return nil
}

// Close terminates connection to persistent storage.
func Close() error {
	if adp.IsOpen() {
		return adp.Close()
	}

	return nil
}

// IsOpen checks if persistent storage connection has been initialized.
func IsOpen() bool {
	if adp != nil {
		return adp.IsOpen()
	}

	return false
}

// GetAdapterName returns the name of the current adater.
func GetAdapterName() string {
	if adp != nil {
		return adp.GetName()
	}

	return ""
}

// RegisterAdapter makes a persistence adapter available.
// If Register is called twice or if the adapter is nil, it panics.
func RegisterAdapter(name string, l adapter.Adapter) {
	if l == nil {
		panic("store: Register adapter is nil")
	}

	if adp != nil {
		panic("store: adapter '" + adp.GetName() + "' is already registered")
	}

	adp = l
}

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct{}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

// handle which outgoing messages are stored
func (l *MessageLog) PersistOutbound(key uint64, msg packets.Packet) {
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Pubcomp:
			// Sending puback. delete matching publish
			// from inbound
			adp.DeleteMessage(key)
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel, *packets.Subscribe, *packets.Unsubscribe:
			// Sending publish. store in obound
			// until puback received
			m, err := packets.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(key, m.Bytes())
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Sending publish. store in outbound
			// until pubrel received
			m, err := packets.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(key, m.Bytes())
		default:
		}
	}
}

// handle which incoming messages are stored
func (l *MessageLog) PersistInbound(key uint64, msg packets.Packet) {
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Suback, *packets.Unsuback, *packets.Pubcomp:
			// Received a puback. delete matching publish
			// from obound
			adp.DeleteMessage(key)
		case *packets.Publish, *packets.Pubrec, *packets.Pingresp, *packets.Connack:
		default:
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel:
			// Received a publish. store it in ibound
			// until puback sent
			m, err := packets.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(key, m.Bytes())
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Received a publish. store it in ibound
			// until pubrel received
			m, err := packets.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(key, m.Bytes())
		default:
		}
	}
}

// Get performs a query and attempts to fetch message for the given key
func (l *MessageLog) Get(key uint64) packets.Packet {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := packets.ReadPacket(r); err == nil {
			return msg
		}
	}
	return nil
}

// Keys performs a query and attempts to fetch all keys.
func (l *MessageLog) Keys() []uint64 {
	return adp.Keys()
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	adp.DeleteMessage(key)
}

// Reset removes all keys from the store.
func (l *MessageLog) Reset() {
	keys := adp.Keys()
	for _, key := range keys {
		adp.DeleteMessage(key)
	}
}
