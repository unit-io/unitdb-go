package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitd-go/packets"
)

// Store represents a message storage contract that message storage provides
// must fulfill.
type Store interface {
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

	// Put is used to store a message.
	// it returns an error if some error was encountered during storage.
	Put(blockId, key uint64, payload []byte) error

	// Get performs a query and attempts to fetch message for the given blockId and key
	Get(blockId, key uint64) ([]byte, error)

	// Keys performs a query and attempts to fetch all keys for given blockId.
	Keys(blockId uint64) []uint64

	// Delete is used to delete message.
	// it returns an error if some error was encountered during delete.
	Delete(blockId, key uint64) error

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

type (
	tinyBatchInfo struct {
		entryCount uint32
	}

	tinyBatch struct {
		tinyBatchInfo
		buffer *bpool.Buffer
	}
)

func (b *tinyBatch) reset() {
	b.entryCount = 0
	atomic.StoreUint32(&b.entryCount, 0)
}

func (b *tinyBatch) count() uint32 {
	return atomic.LoadUint32(&b.entryCount)
}

func (b *tinyBatch) incount() uint32 {
	return atomic.AddUint32(&b.entryCount, 1)
}

// MessageStore is a Message struct to hold methods for persistence mapping for the Message object.
type MessageStore struct {
	seq        uint64
	writeLockC chan struct{}
	bufPool    *bpool.BufferPool
	//tiny Batch
	tinyBatch *tinyBatch
}

// Message is the anchor for storing/retrieving Message objects
var Message MessageStore

func recovery(path string, size int64, reset bool) error {
	m, err := store.Recovery(path, size, reset)
	if err != nil {
		return err
	}
	for k, msg := range m {
		blockId := k & 0xFFFFFFFF
		if err := store.Put(blockId, k, msg); err != nil {
			return err
		}
	}
	return nil
}

// InitDb init message store and start recovery if reset flag is not set.
func InitDb(path string, size int64, dur time.Duration, reset bool) error {
	if !IsOpen() {
		if err := open(path); err != nil {
			return err
		}
	}
	Message = MessageStore{
		writeLockC: make(chan struct{}, 1),
		bufPool:    bpool.NewBufferPool(int64(size), nil),
		tinyBatch:  &tinyBatch{},
	}
	if err := recovery(path, size, reset); err != nil {
		return err
	}
	startLogReleaser(dur)
	return nil
}

// handle which outgoing messages are stored
func (m *MessageStore) PersistOutbound(key uint64, msg packets.Packet) {
	blockId := key & 0xFFFFFFFF
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Pubcomp:
			// Sending puback. delete matching publish
			// from ibound
			store.Delete(blockId, key)
			m.append(true, key, nil)
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel, *packets.Subscribe, *packets.Unsubscribe:
			// Sending publish. store in obound
			// until puback received
			data := msg.Encode()
			store.Put(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Sending publish. store in obound
			// until pubrel received
			data := msg.Encode()
			store.Put(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	}
}

// handle which incoming messages are stored
func (m *MessageStore) PersistInbound(key uint64, msg packets.Packet) {
	blockId := key & 0xFFFFFFFF
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Suback, *packets.Unsuback, *packets.Pubcomp:
			// Received a puback. delete matching publish
			// from obound
			store.Delete(blockId, key)
			m.append(true, key, nil)
		case *packets.Publish, *packets.Pubrec, *packets.Pingresp, *packets.Connack:
		default:
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel:
			// Received a publish. store it in ibound
			// until puback sent
			data := msg.Encode()
			store.Put(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Received a publish. store it in ibound
			// until pubrel received
			data := msg.Encode()
			store.Put(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	}
}

// Get performs a query and attempts to fetch message for the given blockId and key
func (m *MessageStore) Get(key uint64) packets.Packet {
	blockId := key & 0xFFFFFFFF
	if raw, err := store.Get(blockId, key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := packets.ReadPacket(r); err == nil {
			return msg
		}
	}
	return nil
}

// Keys performs a query and attempts to fetch all keys for given blockId and key prefix.
func (m *MessageStore) Keys(prefix uint32) []uint64 {
	matches := make([]uint64, 0)
	keys := store.Keys(uint64(prefix))
	for _, k := range keys {
		if evalPrefix(k, prefix) {
			matches = append(matches, k)
		}
	}
	return matches
}

// Delete is used to delete message.
func (m *MessageStore) Delete(key uint64) {
	blockId := key & 0xFFFFFFFF
	store.Delete(blockId, key)
	m.append(true, key, nil)
}

// Reset removes all keys from store for the given blockId and key prefix
func (m *MessageStore) Reset(prefix uint32) {
	keys := store.Keys(uint64(prefix))
	for _, k := range keys {
		if evalPrefix(k, prefix) {
			store.Delete(uint64(prefix), k)
		}
	}
}

// append appends message to tinyBatch for writing to log file.
func (m *MessageStore) append(delFlag bool, k uint64, data []byte) error {
	var dBit uint8
	if delFlag {
		dBit = 1
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+8+4+1))

	if _, err := m.tinyBatch.buffer.Write(scratch[:]); err != nil {
		return err
	}

	// key with flag bit
	var key [9]byte
	key[0] = dBit
	binary.LittleEndian.PutUint64(key[1:], k)
	if _, err := m.tinyBatch.buffer.Write(key[:]); err != nil {
		return err
	}
	if _, err := m.tinyBatch.buffer.Write(data); err != nil {
		return err
	}

	m.tinyBatch.incount()
	return nil
}

// tinyCommit commits tiny batch to log file
func (m *MessageStore) tinyCommit() error {
	if m.tinyBatch.count() == 0 {
		return nil
	}

	if err := store.NewWriter(); err != nil {
		return err
	}
	// commit writes batches into write ahead log. The write happen synchronously.
	m.writeLockC <- struct{}{}
	defer func() {
		m.tinyBatch.buffer.Reset()
		<-m.writeLockC
	}()
	offset := uint32(0)
	buf := m.tinyBatch.buffer.Bytes()
	for i := uint32(0); i < m.tinyBatch.count(); i++ {
		dataLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		data := buf[offset+4 : offset+dataLen]
		if err := <-store.Append(data); err != nil {
			return err
		}
		offset += dataLen
	}

	if err := <-store.SignalInitWrite(timeNow()); err != nil {
		return err
	}
	m.tinyBatch.reset()
	return nil
}

func evalPrefix(key uint64, prefix uint32) bool {
	return uint64(prefix) == key&0xFFFFFFFF
}

func timeNow() uint64 {
	return uint64(time.Now().UTC().Round(time.Millisecond).Unix())
}

func timeSeq(dur time.Duration) uint64 {
	return uint64(time.Now().UTC().Truncate(dur).Round(time.Millisecond).Unix())
}

// logs are released from wal if older than a minute.
func startLogReleaser(dur time.Duration) {
	ctx := context.Background()
	go func() {
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				store.SignalLogApplied(timeSeq(dur))
			}
		}
	}()
}
