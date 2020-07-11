package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	adapter "github.com/unit-io/unitd-go/db"
	"github.com/unit-io/unitd-go/packets"
)

var adp adapter.Adapter

func open(path string) error {
	if adp == nil {
		return errors.New("store: database adapter is missing")
	}

	if adp.IsOpen() {
		return errors.New("store: connection is already opened")
	}

	return adp.Open(path)
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

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct {
	seq        uint64
	writeLockC chan struct{}
	bufPool    *bpool.BufferPool
	//tiny Batch
	tinyBatch *tinyBatch
}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

func recovery(path string, size int64, reset bool) error {
	m, err := adp.Recovery(path, size, reset)
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
func InitMessageStore(ctx context.Context, path string, size int64, dur time.Duration, reset bool) error {
	if !IsOpen() {
		if err := open(path); err != nil {
			return err
		}
	}
	Log = MessageLog{
		writeLockC: make(chan struct{}, 1),
		bufPool:    bpool.NewBufferPool(int64(size), nil),
		tinyBatch:  &tinyBatch{},
	}
	Log.tinyBatch.buffer = Log.bufPool.Get()
	if err := recovery(path, size, reset); err != nil {
		return err
	}
	Log.tinyBatchLoop(ctx, 15*time.Millisecond)
	logReleaser(ctx, dur)
	return nil
}

// handle which outgoing messages are stored
func (m *MessageLog) PersistOutbound(key uint64, msg packets.Packet) {
	blockId := key & 0xFFFFFFFF
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Pubcomp:
			// Sending puback. delete matching publish
			// from ibound
			adp.DeleteMessage(blockId, key)
			m.append(true, key, nil)
		}
	case 1:
		switch msg.(type) {
		case *packets.Publish, *packets.Pubrel, *packets.Subscribe, *packets.Unsubscribe:
			// Sending publish. store in obound
			// until puback received
			data := msg.Encode()
			adp.PutMessage(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Sending publish. store in obound
			// until pubrel received
			data := msg.Encode()
			adp.PutMessage(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	}
}

// handle which incoming messages are stored
func (m *MessageLog) PersistInbound(key uint64, msg packets.Packet) {
	blockId := key & 0xFFFFFFFF
	switch msg.Info().Qos {
	case 0:
		switch msg.(type) {
		case *packets.Puback, *packets.Suback, *packets.Unsuback, *packets.Pubcomp:
			// Received a puback. delete matching publish
			// from obound
			adp.DeleteMessage(blockId, key)
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
			adp.PutMessage(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	case 2:
		switch msg.(type) {
		case *packets.Publish:
			// Received a publish. store it in ibound
			// until pubrel received
			data := msg.Encode()
			adp.PutMessage(blockId, key, data)
			m.append(false, key, data)
		default:
		}
	}
}

// Get performs a query and attempts to fetch message for the given blockId and key
func (m *MessageLog) Get(key uint64) packets.Packet {
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
func (m *MessageLog) Keys(prefix uint32) []uint64 {
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
func (m *MessageLog) Delete(key uint64) {
	blockId := key & 0xFFFFFFFF
	adp.DeleteMessage(blockId, key)
	m.append(true, key, nil)
}

// Reset removes all keys from store for the given blockId and key prefix
func (m *MessageLog) Reset(prefix uint32) {
	keys := adp.Keys(uint64(prefix))
	for _, k := range keys {
		if evalPrefix(k, prefix) {
			adp.DeleteMessage(uint64(prefix), k)
		}
	}
}

// append appends message to tinyBatch for writing to log file.
func (m *MessageLog) append(delFlag bool, k uint64, data []byte) error {
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
	if data != nil {
		if _, err := m.tinyBatch.buffer.Write(data); err != nil {
			return err
		}
	}

	m.tinyBatch.incount()
	return nil
}

// tinyCommit commits tiny batch to log file
func (m *MessageLog) tinyCommit() error {
	if m.tinyBatch.count() == 0 {
		return nil
	}

	if err := adp.NewWriter(); err != nil {
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
		if err := <-adp.Append(data); err != nil {
			return err
		}
		offset += dataLen
	}

	if err := <-adp.SignalInitWrite(timeNow()); err != nil {
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

// tinyBatchLoop handles tiny bacthes write to log.
func (m *MessageLog) tinyBatchLoop(ctx context.Context, interval time.Duration) {
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
				if err := m.tinyCommit(); err != nil {
					fmt.Println("Error committing tinyBatch")
				}
			}
		}
	}()
}

// logs are released from wal if older than a minute.
func logReleaser(ctx context.Context, dur time.Duration) {
	go func() {
		logTicker := time.NewTicker(dur)
		defer logTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-logTicker.C:
				adp.SignalLogApplied(timeSeq(dur))
			}
		}
	}()
}
