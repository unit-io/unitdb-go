package unitdb

import (
	"context"
	"sync"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
)

// MessageAndResult is a type that contains both a Message and a Result.
// This type is passed via channels between client connection interface and
// goroutines responsible for sending and receiving messages from server
type MessageAndResult struct {
	m lp.MessagePack
	r Result
}

type Result interface {
	flowComplete()
	Get(ctx context.Context, d time.Duration) (bool, error)
}

type result struct {
	m        sync.RWMutex
	complete chan struct{}
	err      error
}

func (r *result) flowComplete() {
	select {
	case <-r.complete:
	default:
		close(r.complete)
	}
}

func (r *result) setError(err error) {
	r.m.Lock()
	defer r.m.Unlock()
	r.err = err
	r.flowComplete()
}

func (r *result) error() error {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.err
}

// Get returns if server call is complete with error result of call
// Get blocks until server call is complete or context is done or till duration specified
func (r *result) Get(ctx context.Context, d time.Duration) (bool, error) {
	// If result is already complete, return it even if the context is done
	select {
	case <-r.complete:
		return true, r.error()
	default:
	}

	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		return true, r.error()
	case <-r.complete:
		if !timer.Stop() {
			<-timer.C
		}
		return true, r.error()
	case <-timer.C:
	}
	return false, r.error()
}

// ConnectResult is an extension of result containing extra fields
// it provides information about calls to Connect()
type ConnectResult struct {
	result
	returnCode     int32
	sessionPresent bool
}

// ReturnCode returns the acknowledgement code in the connack sent
// in response to a Connect()
func (r *ConnectResult) ReturnCode() int32 {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.returnCode
}

// SessionPresent returns a bool representing the value of the
// session present field in the connack sent in response to a Connect()
func (r *ConnectResult) SessionPresent() bool {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.sessionPresent
}

// PublishResult is an extension of result containing the extra fields
// required to provide information about calls to Publish()
type PublishResult struct {
	result
	messageID uint16
}

// MessageID returns the message ID that was assigned to the
// Publish Message when it was sent to the server
func (r *PublishResult) MessageID() uint16 {
	return r.messageID
}

// RelayResult is an extension of result containing the extra fields
// required to provide information about calls to Relay()
type RelayResult struct {
	result
	reqs      []*utp.RelayRequest
	relResult map[string]byte
	messageID uint16
}

// Result returns a map of topics that were requested to along with
// the matching return code from the server.
func (r *RelayResult) Result() map[string]byte {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.relResult
}

// // Subscription is a struct for pairing the DeliveryMode and topic together
// // for the delivery mode's pairs in unsubscribe and subscribe
// type Subscription struct {
// 	DeliveryMode int32
// 	Topic        string
// }

// SubscribeResult is an extension of result containing the extra fields
// required to provide information about calls to Subscribe()
type SubscribeResult struct {
	result
	// subs      []*Subscription
	subs      []*utp.Subscription
	subResult map[string]byte
	messageID uint16
}

// Result returns a map of topics that were subscribed to along with
// the matching return code from the server. This is either the DeliveryMode
// value of the subscription or an error code.
func (r *SubscribeResult) Result() map[string]byte {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.subResult
}

// UnsubscribeResult is an extension of result containing the extra fields
// required to provide information about calls to Unsubscribe()
type UnsubscribeResult struct {
	result
	messageID int32
}

// DisconnectResult is an extension of result containing the extra fields
// required to provide information about calls to Disconnect()
type DisconnectResult struct {
	result
}

// PutResult is an extension of result containing the extra fields
// required to provide information about calls to Put()
type PutResult struct {
	result
	messageID uint16
}

// MessageID returns the message ID that was assigned to the
// Publish Message when it was sent to the server
func (r *PutResult) MessageID() uint16 {
	return r.messageID
}
