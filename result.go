package unitdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	// req is set for a request to the server's API.
	req *pendingRequest
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

// RequestResult is an extension of result for a request to the server's
// API: Revoke, RevokeAll and Vouch, and the base of KeygenResult and
// ClientIDResult. It completes when the server answers; Get returns a
// *RequestError if the server refused the request.
type RequestResult struct {
	result
	status  int
	message string
}

func newRequestResult() *RequestResult {
	return &RequestResult{result: result{complete: make(chan struct{})}}
}

// Status returns the status the server answered with, as an HTTP status:
// 200 once the request succeeded, 0 before the server answers.
func (r *RequestResult) Status() int {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.status
}

// Message returns the server's explanation of a refusal, if any.
func (r *RequestResult) Message() string {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.message
}

// answer completes the result with an answer that holds a status only.
func (r *RequestResult) answer(payload []byte) {
	s, err := answerStatus(payload)
	r.completeWith(s, err)
}

// completeWith records the answer's status and completes the result.
func (r *RequestResult) completeWith(s status, err error) {
	r.m.Lock()
	r.status, r.message = s.Status, s.Message
	r.m.Unlock()
	if err != nil {
		r.setError(err)
		return
	}
	r.flowComplete()
}

// KeygenResult is an extension of RequestResult for Keygen.
type KeygenResult struct {
	*RequestResult
	keys []TopicKey
}

// Keys returns the keys the server issued, one per KeyRequest, in order.
func (r *KeygenResult) Keys() []TopicKey {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.keys
}

func (r *KeygenResult) answer(payload []byte) {
	// A refusal is a status; keys are a list.
	if trimmed := bytes.TrimSpace(payload); len(trimmed) == 0 || trimmed[0] != '[' {
		r.RequestResult.answer(payload)
		return
	}
	var resp []struct {
		Status  int    `json:"status"`
		Message string `json:"message"`
		Key     string `json:"key"`
		Topic   string `json:"topic"`
		UUID    string `json:"uuid"`
	}
	if err := json.Unmarshal(payload, &resp); err != nil {
		r.completeWith(status{}, errors.New("unexpected answer from the server: "+string(payload)))
		return
	}
	s := status{Status: 200}
	keys := make([]TopicKey, 0, len(resp))
	for _, k := range resp {
		if k.Status != 200 {
			s = status{Status: k.Status, Message: k.Message}
			continue
		}
		keys = append(keys, TopicKey{Topic: k.Topic, Key: k.Key, UUID: k.UUID})
	}
	r.m.Lock()
	r.keys = keys
	r.m.Unlock()
	r.completeWith(s, s.refused())
}

// ClientIDResult is an extension of RequestResult for RequestClientID.
type ClientIDResult struct {
	*RequestResult
	clientID string
	uuid     string
}

// ClientID returns the client id the server issued.
func (r *ClientIDResult) ClientID() string {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.clientID
}

// UUID returns the client id's uuid in decimal, to revoke it with (see
// Client.Revoke), or "" for a v1 id, which has none: only a v0.6.0 server,
// in a cluster with nodes that don't read v2 ids, still issues one.
func (r *ClientIDResult) UUID() string {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.uuid
}

func (r *ClientIDResult) answer(payload []byte) {
	var resp struct {
		status
		Key  string `json:"key"`
		UUID string `json:"uuid"`
	}
	if err := json.Unmarshal(payload, &resp); err != nil {
		r.completeWith(status{}, errors.New("unexpected answer from the server: "+string(payload)))
		return
	}
	r.m.Lock()
	r.clientID, r.uuid = resp.Key, resp.UUID
	r.m.Unlock()
	r.completeWith(resp.status, resp.status.refused())
}
