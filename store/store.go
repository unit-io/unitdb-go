package store

import (
	"bytes"
	"errors"
	"fmt"

	adapter "github.com/unit-io/unitdb-go/db"
	"github.com/unit-io/unitdb-go/utp"
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
func (l *MessageLog) PersistOutbound(blockID uint32, msg utp.Packet) {
	switch msg.Info().DeliveryMode {
	case 0:
		switch msg.(type) {
		case *utp.Pubcomplete:
			// Sending pubcomp. delete matching publish
			// from inbound
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			adp.DeleteMessage(ikey)
		case *utp.Publish:
			// Sending publish. store in obound
			// until pubcomp received
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			m, err := utp.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(okey, m.Bytes())
		}
	case 1, 2:
		switch msg.(type) {
		case *utp.Pubreceipt:
			// Sending pubrec. delete matching pubnew for RELIABLE delivery mode
			// from inbound
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			adp.DeleteMessage(ikey)
		case *utp.Publish, *utp.Pubreceive, *utp.Subscribe, *utp.Unsubscribe:
			// Sending publish. store in obound
			// until pubcomp received
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			m, err := utp.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(okey, m.Bytes())
		default:
			fmt.Println("Store::PersistOutbound: Invalid message type")
		}
	}
}

// handle which incoming messages are stored
func (l *MessageLog) PersistInbound(blockID uint32, msg utp.Packet) {
	switch msg.Info().DeliveryMode {
	case 0:
		switch msg.(type) {
		case *utp.Pubcomplete, *utp.Suback, *utp.Unsuback:
			// Received a pubcomp. delete matching publish for EXPRESS delivery mode
			// or pubrecv for RELIABLE delivery mode from obound
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			adp.DeleteMessage(okey)
		case *utp.Publish:
			// Received a publish. store it in ibound
			// until pubcomp received
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			m, err := utp.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(ikey, m.Bytes())
		case *utp.Pingresp, *utp.Connack:
		default:
			fmt.Println("Store::PersistInbound: Invalid message type")
		}
	case 1, 2:
		switch msg.(type) {
		case *utp.Pubreceipt:
			// Received a pubrec. delete matching publish
			// from obound
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			adp.DeleteMessage(okey)
		case *utp.Pubnew:
			// Received a publish. store it in ibound
			// until pubrec received
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			m, err := utp.Encode(msg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(ikey, m.Bytes())
		case *utp.Publish:
		default:
			fmt.Println("Store::PersistInbound: Invalid message type")
		}
	}
}

// Get performs a query and attempts to fetch message for the given key
func (l *MessageLog) Get(key uint64) utp.Packet {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := utp.ReadPacket(r); err == nil {
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
