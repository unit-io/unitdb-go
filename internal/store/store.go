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
	return adp.Close()
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

// Log keys hold the block (session) id in the high 32 bits and the message
// id in the low 16 bits. Bit 16 is set for entries about messages the server
// sent, since their ids are allocated independently of the client's own.
const inboundKey = 1 << 16

func logKey(blockID uint32, messageID uint16, inbound bool) uint64 {
	key := uint64(blockID)<<32 | uint64(messageID)
	if inbound {
		key |= inboundKey
	}
	return key
}

// IsInboundKey reports whether a log key is about a message the server sent.
func IsInboundKey(key uint64) bool {
	return key&inboundKey != 0
}

func putMessage(key uint64, m lp.MessagePack) {
	buf, err := lp.Encode(m)
	if err != nil {
		fmt.Println(err)
		return
	}
	adp.PutMessage(key, buf.Bytes())
}

// PersistOutbound logs the outgoing messages that wait for the server.
func (l *MessageLog) PersistOutbound(blockID uint32, outMsg lp.MessagePack) {
	switch m := outMsg.(type) {
	case *utp.Publish, *utp.Subscribe, *utp.Unsubscribe:
		// Kept until the server acknowledges it.
		putMessage(logKey(blockID, outMsg.Info().MessageID, false), outMsg)
	case *utp.ControlMessage:
		if m.FlowControl == utp.RECEIPT {
			// A receipt for a message of the server, kept until the server
			// completes the flow. It replaces the stored notify.
			putMessage(logKey(blockID, m.MessageID, true), outMsg)
		}
	}
}

// PersistInbound logs the incoming messages that wait for the client and
// removes the entries whose flow the server completed.
func (l *MessageLog) PersistInbound(blockID uint32, inMsg lp.MessagePack) {
	switch m := inMsg.(type) {
	case *utp.Publish:
		// A reliable message is kept until its receipt is sent; an express
		// message needs nothing more.
		if m.DeliveryMode != 0 {
			putMessage(logKey(blockID, m.MessageID, true), inMsg)
		}
	case *utp.ControlMessage:
		switch m.FlowControl {
		case utp.ACKNOWLEDGE:
			// The server acknowledged one of the client's messages.
			adp.DeleteMessage(logKey(blockID, m.MessageID, false))
		case utp.COMPLETE:
			// The server completed the flow of one of its messages.
			adp.DeleteMessage(logKey(blockID, m.MessageID, true))
		case utp.NOTIFY:
			// Kept until the message is received.
			putMessage(logKey(blockID, m.MessageID, true), inMsg)
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
	return uint32(key>>32) == prefix
}
