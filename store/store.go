package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	adapter "github.com/unit-io/unite-go/db"
	"github.com/unit-io/unite-go/packets"
)

var adp adapter.Adapter

func open(path string, size int64, dur time.Duration) error {
	if adp == nil {
		return errors.New("store: database adapter is missing")
	}

	if adp.IsOpen() {
		return errors.New("store: connection is already opened")
	}

	return adp.Open(path, size, dur)
}

// Open initializes the persistence. Adapter holds a connection pool for a database instance.
//   path - database path
func Open(path string, size int64, dur time.Duration) error {
	if err := open(path, size, dur); err != nil {
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

func recovery(reset bool) error {
	m, err := adp.Recovery(reset)
	if err != nil {
		return err
	}
	for k, msg := range m {
		blockId := k & 0xFFFFFFFF
		if err := adp.PutMessage(blockId, k, msg); err != nil {
			return err
		}
	}
	return nil
}

// InitMessageStore init message store and start recovery if reset flag is not set.
func InitMessageStore(ctx context.Context, reset bool) error {
	if err := recovery(reset); err != nil {
		return err
	}
	writeLoop(ctx, 15*time.Millisecond)
	return nil
}

// handle which outgoing messages are stored
func (l *MessageLog) PersistOutbound(blockId, key uint64, msg packets.Packet) {
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Pubcomp:
			// Sending puback. delete matching publish
			// from inbound
			adp.DeleteMessage(blockId, key)
			adp.Append(true, key, nil)
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
			adp.PutMessage(blockId, key, m.Bytes())
			adp.Append(false, key, m.Bytes())
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
			adp.PutMessage(blockId, key, m.Bytes())
			adp.Append(false, key, m.Bytes())
		default:
		}
	}
}

// handle which incoming messages are stored
func (l *MessageLog) PersistInbound(blockId, key uint64, msg packets.Packet) {
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Suback, *packets.Unsuback, *packets.Pubcomp:
			// Received a puback. delete matching publish
			// from obound
			adp.DeleteMessage(blockId, key)
			adp.Append(true, key, nil)
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
			adp.PutMessage(blockId, key, m.Bytes())
			adp.Append(false, key, m.Bytes())
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
			adp.PutMessage(blockId, key, m.Bytes())
			adp.Append(false, key, m.Bytes())
		default:
		}
	}
}

// Get performs a query and attempts to fetch message for the given blockId and key
func (l *MessageLog) Get(key uint64) packets.Packet {
	blockId := key & 0xFFFFFFFF
	if raw, err := adp.GetMessage(blockId, key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := packets.ReadPacket(r); err == nil {
			return msg
		}
	}
	return nil
}

// Keys performs a query and attempts to fetch all keys for given blockId and key prefix.
func (l *MessageLog) Keys(prefix uint32) []uint64 {
	matches := make([]uint64, 0)
	keys := adp.Keys(uint64(prefix))
	for _, k := range keys {
		if evalPrefix(k, prefix) {
			matches = append(matches, k)
		}
	}
	return matches
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	blockId := key & 0xFFFFFFFF
	adp.DeleteMessage(blockId, key)
	adp.Append(true, key, nil)
}

// Reset removes all keys from store for the given blockId and key prefix
func (l *MessageLog) Reset(prefix uint32) {
	keys := adp.Keys(uint64(prefix))
	for _, k := range keys {
		if evalPrefix(k, prefix) {
			adp.DeleteMessage(uint64(prefix), k)
		}
	}
}

func evalPrefix(key uint64, prefix uint32) bool {
	return uint64(prefix) == key&0xFFFFFFFF
}

// writeLoop handles writing to log file.
func writeLoop(ctx context.Context, interval time.Duration) {
	go func() {
		tinyBatchWriterTicker := time.NewTicker(interval)
		defer func() {
			tinyBatchWriterTicker.Stop()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tinyBatchWriterTicker.C:
				if err := adp.Write(); err != nil {
					fmt.Println("Error committing tinyBatch")
				}
			}
		}
	}()
}
