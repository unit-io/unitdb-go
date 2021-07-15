package store

import (
	"bytes"
	"errors"
	"fmt"

	adapter "github.com/unit-io/unitdb-go/internal/db"
	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
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

// SessionStore is a Session struct to hold methods for persistence mapping for the Session object.
type SessionStore struct{}

// Session is the anchor for storing/retrieving Session objects
var Session SessionStore

func (s *SessionStore) Put(key uint64, payload []byte) error {
	return adp.PutMessage(key, payload)
}

func (s *SessionStore) Get(key uint64) (raw []byte, err error) {
	return adp.GetMessage(key)
}

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct{}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

// handle which outgoing messages are stored
func (l *MessageLog) PersistOutbound(blockID uint32, outMsg lp.MessagePack) {
	switch outMsg.(type) {
	case *utp.Publish, *utp.Subscribe, *utp.Unsubscribe:
		// Received a publish. store it in ibound
		// until ACKNOWLEDGE or COMPLETE is sent
		okey := uint64(blockID)<<32 + uint64(outMsg.Info().MessageID)
		m, err := lp.Encode(outMsg)
		if err != nil {
			fmt.Println(err)
			return
		}
		adp.PutMessage(okey, m.Bytes())
	}
	if outMsg.Type() == utp.FLOWCONTROL {
		msg := *outMsg.(*utp.ControlMessage)
		switch msg.FlowControl {
		case utp.RECEIPT:
			// Received a RECEIPT control message. store it in obound
			// until COMPLETE is received.
			okey := uint64(blockID)<<32 + uint64(outMsg.Info().MessageID)
			m, err := lp.Encode(outMsg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(okey, m.Bytes())
		}
	}
}

// handle which incoming messages are stored
func (l *MessageLog) PersistInbound(blockID uint32, inMsg lp.MessagePack) {
	switch inMsg.(type) {
	case *utp.Publish:
		// Received a publish. store it in ibound
		// until COMPLETE sent
		ikey := uint64(blockID)<<32 + uint64(inMsg.Info().MessageID)
		m, err := lp.Encode(inMsg)
		if err != nil {
			fmt.Println(err)
			return
		}
		adp.PutMessage(ikey, m.Bytes())
	}
	if inMsg.Type() == utp.FLOWCONTROL {
		msg := *inMsg.(*utp.ControlMessage)
		switch msg.FlowControl {
		case utp.ACKNOWLEDGE, utp.COMPLETE:
			// Sending ACKNOWLEDGE, delete matching PUBLISH for EXPRESS delivery mode
			// or sending COMPLETE, delete matching RECEIVE for RELIABLE delivery mode from ibound
			okey := uint64(blockID)<<32 + uint64(inMsg.Info().MessageID)
			adp.DeleteMessage(okey)
		case utp.NOTIFY:
			// Sending RECEIPT. store in obound
			// until COMPLETE received
			ikey := uint64(blockID)<<32 + uint64(inMsg.Info().MessageID)
			m, err := lp.Encode(inMsg)
			if err != nil {
				fmt.Println(err)
				return
			}
			adp.PutMessage(ikey, m.Bytes())
		}
	}
}

// Get performs a query and attempts to fetch message for the given key
func (l *MessageLog) Get(key uint64) lp.MessagePack {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := lp.Read(r); err == nil {
			return msg
		}
	}
	return nil
}

// Keys performs a query and attempts to fetch all keys that matches prefix.
func (l *MessageLog) Keys(prefix uint32) []uint64 {
	matches := make([]uint64, 0)
	keys := adp.Keys()
	for _, key := range keys {
		if evalPrefix(prefix, key) {
			matches = append(matches, key)
		}
	}
	return matches
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	adp.DeleteMessage(key)
}

// Reset removes all keys with the prefix from the store.
func (l *MessageLog) Reset(prefix uint32) {
	keys := adp.Keys()
	for _, key := range keys {
		if evalPrefix(prefix, key) {
			adp.DeleteMessage(key)
		}
	}
}

func evalPrefix(prefix uint32, key uint64) bool {
	return uint64(prefix) == key&0xFFFFFFFF
}
