package unitdb

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

// Topics of the server's API. A request is published on its topic, and the
// server answers it on the same topic, in the order the requests came.
const (
	topicKeygen   = "unitdb/keygen"
	topicClientID = "unitdb/clientid"
	topicRevoke   = "unitdb/revoke"
	topicService  = "unitdb/service"

	// topicNewClientID is where the server sends a client its new or
	// renewed client id.
	topicNewClientID = "unitdb/clientid/"
)

// KeyRequest asks the server for a topic key (see Client.Keygen).
type KeyRequest struct {
	// Topic is the topic, or wildcard topic, the key opens.
	Topic string
	// Type is the key's access: any of "r" (read), "w" (write), "a"
	// (admin) and "o" (owner), as in "rw".
	Type string
	// TTL is how long the key lasts. 0 leaves it to the server: its
	// topic_key_ttl, for ever by default.
	TTL time.Duration
}

// TopicKey is a topic key the server issued.
type TopicKey struct {
	// Topic is the topic the key opens.
	Topic string
	// Key is the key, to prefix the topic with, as in Key+"/"+Topic. It is
	// opaque: a v2 key is 48 characters of base64url, which include '-'
	// and '_'.
	Key string
	// UUID is the key's uuid in decimal, to revoke it with (see
	// Client.Revoke). A v1 key, which a cluster with nodes that don't read
	// v2 keys still issues, has none.
	UUID string
}

// RequestError is the error of a request the server refused (see
// RequestResult).
type RequestError struct {
	// Status is the status the server answered with, as an HTTP status.
	Status int
	// Message is the server's explanation, if any.
	Message string
}

func (e *RequestError) Error() string {
	if e.Message == "" {
		return "request refused with status " + strconv.Itoa(e.Status)
	}
	return "request refused with status " + strconv.Itoa(e.Status) + ": " + e.Message
}

// errRequestConnLost fails the requests the server had not answered when
// the connection was lost.
var errRequestConnLost = errors.New("connection lost before the server answered the request")

// requestResult is the result of a request to the server's API.
type requestResult interface {
	Result
	// answer completes the result with the server's answer.
	answer(payload []byte)
	setError(err error)
}

// status is the answer the server gives to every request, and when it
// refuses one.
type status struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

// refused returns the error of an answer, if it is a refusal.
func (s status) refused() error {
	if s.Status == 200 {
		return nil
	}
	return &RequestError{Status: s.Status, Message: s.Message}
}

// answerStatus decodes an answer that holds a status only.
func answerStatus(payload []byte) (status, error) {
	var s status
	if err := json.Unmarshal(payload, &s); err != nil {
		return s, errors.New("unexpected answer from the server: " + string(payload))
	}
	return s, s.refused()
}

// Keygen asks the server for topic keys. Only a primary client id, or a
// connection trusted as a service's, may. The result is a *KeygenResult.
func (c *client) Keygen(reqs ...KeyRequest) Result {
	r := &KeygenResult{RequestResult: newRequestResult()}
	type keyRequest struct {
		Topic string `json:"topic"`
		Type  string `json:"type"`
		TTL   string `json:"ttl,omitempty"`
	}
	body := make([]keyRequest, 0, len(reqs))
	for _, req := range reqs {
		kr := keyRequest{Topic: req.Topic, Type: req.Type}
		if req.TTL < 0 {
			r.setError(errors.New("keygen: negative ttl"))
			return r
		}
		if req.TTL > 0 {
			kr.TTL = req.TTL.String()
		}
		body = append(body, kr)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		r.setError(err)
		return r
	}
	c.request(topicKeygen, payload, r)
	return r
}

// RequestClientID asks the server for a new secondary client id of the
// client's contract. Only a primary client id may. The result is a
// *ClientIDResult.
func (c *client) RequestClientID() Result {
	r := &ClientIDResult{RequestResult: newRequestResult()}
	c.request(topicClientID, nil, r)
	return r
}

// Revoke revokes the client id or topic key of the client's contract with
// the uuid, as Keygen and RequestClientID give it, until the time given, or
// for ever if it is zero. Only a primary client id may revoke. A revoked
// client id is refused when it connects; a revoked key when it is used.
// What is already open, connections and subscriptions, stays open. The
// result is a *RequestResult.
func (c *client) Revoke(uuid string, until time.Time) Result {
	r := newRequestResult()
	req := struct {
		UUID  string `json:"uuid"`
		Until int64  `json:"until,omitempty"`
	}{UUID: uuid}
	if !until.IsZero() {
		req.Until = until.Unix()
	}
	payload, _ := json.Marshal(req)
	c.request(topicRevoke, payload, r)
	return r
}

// RevokeAll revokes every client id and topic key the client's contract was
// issued before now, in whole seconds, and every v1 one, which carry no
// issue time. That includes the client's own id: it is refused the next
// time it connects, and its contract's clients need ids the primary client
// requests after this. Only a primary client id may. The result is a
// *RequestResult.
func (c *client) RevokeAll() Result {
	r := newRequestResult()
	c.request(topicRevoke, []byte(`{"all":true}`), r)
	return r
}

// Vouch vouches for the connection with a trusted service's client id of the
// client's contract: the connection then needs no topic keys, until it
// ends. A service that connects for its users vouches for each connection;
// keep the service's id on the service, never on clients or devices. Vouch
// for each new connection again: after a reconnect, the server knows the
// connection no more. The result is a *RequestResult.
func (c *client) Vouch(serviceID string) Result {
	r := newRequestResult()
	payload, _ := json.Marshal(struct {
		ClientID string `json:"client_id"`
	}{serviceID})
	c.request(topicService, payload, r)
	return r
}

// pendingRequest is a request to the server's API not answered yet.
type pendingRequest struct {
	topic string
	r     requestResult
	// gen is the connection the request was written on, 0 until then.
	gen uint64
	// failed is set when the request failed before it was written: it is
	// not written then.
	failed bool
}

// request publishes a request to the server's API on topic, and queues r to
// be completed by the server's answer.
//
// Requests are not stored in the client's log, and are not sent again: one
// the server has not answered when its connection is lost fails. Answers are
// matched to requests by their order, so an application that uses these
// requests should not also publish to the server's API topics itself.
func (c *client) request(topic string, payload []byte, r requestResult) {
	if err := c.ok(); err != nil {
		r.setError(err)
		return
	}
	// The server acknowledges the publish after it answers it: the answer,
	// not the acknowledgement, completes r.
	ack := &PublishResult{result: result{complete: make(chan struct{})}}
	pub := &utp.Publish{Messages: []*utp.PublishMessage{{Topic: topic, Payload: payload}}}
	pub.MessageID = c.outboundID(c.nextID(ack))

	req := &pendingRequest{topic: topic, r: r}
	c.reqMu.Lock()
	c.pending = append(c.pending, req)
	c.reqMu.Unlock()

	if !c.sendMessage(&MessageAndResult{m: pub, r: ack, req: req}) {
		c.failRequests(0, errClientClosed)
	}
}

// writingRequest records that req is written on connection gen, and reports
// whether to write it: not if it failed already.
func (c *client) writingRequest(req *pendingRequest, gen uint64) bool {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()
	if req.failed {
		return false
	}
	req.gen = gen
	return true
}

// failRequests fails the requests written on connection gen, which the
// server won't answer, or every request if gen is 0.
func (c *client) failRequests(gen uint64, err error) {
	var failed []*pendingRequest
	c.reqMu.Lock()
	kept := c.pending[:0]
	for _, req := range c.pending {
		if gen == 0 || req.gen == gen {
			req.failed = true
			failed = append(failed, req)
			continue
		}
		kept = append(kept, req)
	}
	for i := len(kept); i < len(c.pending); i++ {
		c.pending[i] = nil
	}
	c.pending = kept
	c.reqMu.Unlock()
	for _, req := range failed {
		req.r.setError(err)
	}
}

// answered takes the request an answer on topic, received on connection
// gen, answers: the server answers the requests of a connection in order.
func (c *client) answered(gen uint64, topic string) requestResult {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()
	for i, req := range c.pending {
		if req.gen == gen && req.topic == topic {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return req.r
		}
	}
	return nil
}

// serverMessages handles the messages of a publish the server sends on
// connection gen for the client itself: a renewed client id, and answers to
// requests. They are passed on to the topic filters as well.
func (c *client) serverMessages(gen uint64, pub *utp.Publish) {
	for _, m := range pub.Messages {
		switch m.Topic {
		case topicNewClientID:
			c.renewClientID(string(m.Payload))
		case topicKeygen, topicClientID, topicRevoke, topicService:
			if r := c.answered(gen, m.Topic); r != nil {
				r.answer(m.Payload)
			}
		}
	}
}

// ClientID returns the client id the client connects with: the one it was
// created with, or the server renewed it with since.
func (c *client) ClientID() string {
	c.idMu.Lock()
	defer c.idMu.Unlock()
	return c.opts.clientID
}

// renewClientID adopts the client id the server renewed the client's with:
// the same id sealed again, with a new expiry. The client connects with it
// from then on, keeps its local store, and tells the application.
func (c *client) renewClientID(id string) {
	if !validClientID(id) {
		return
	}
	c.idMu.Lock()
	old := c.opts.clientID
	if id == old || old == "" {
		c.idMu.Unlock()
		return
	}
	c.opts.clientID = id
	dir := c.storeDir
	c.idMu.Unlock()

	// A client created with the new id finds the store of this one.
	if dir != "" {
		linkStore(c.opts.storePath, id, dir)
	}
	if c.opts.clientIDHandler != nil {
		go c.opts.clientIDHandler(c, id)
	}
}

// validClientID reports whether id can be a client id: v2 ids are base64url,
// v1 ones base32. The id names a file of the store, so it must not hold a
// path separator.
func validClientID(id string) bool {
	if id == "" || len(id) > 255 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_', r == '=':
		default:
			return false
		}
	}
	return true
}

// The local store of a client lives in a directory named after its client
// id, under the store path. A renewed id gets a link instead: a small file,
// named after the id, holding the name of the store's directory. A store
// keeps its directory across any number of renewals.
const storeLinkPrefix = "unitdb-store-link:"

// resolveStore returns the directory of the store of client id under root.
func resolveStore(root, id string) string {
	if id == "" {
		return root
	}
	path := filepath.Join(root, id)
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Size() > 1024 {
		return path
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return path
	}
	name, ok := strings.CutPrefix(strings.TrimSpace(string(b)), storeLinkPrefix)
	if !ok || name == "" || name != filepath.Base(name) || name == "." || name == ".." {
		return path
	}
	return filepath.Join(root, name)
}

// linkStore links client id to the store directory dir under root, unless id
// already has a store or a link of its own.
func linkStore(root, id, dir string) error {
	path := filepath.Join(root, id)
	if path == dir {
		return nil
	}
	if _, err := os.Lstat(path); err == nil {
		return nil
	}
	tmp, err := os.CreateTemp(root, ".link-*")
	if err != nil {
		return err
	}
	_, err = tmp.WriteString(storeLinkPrefix + filepath.Base(dir) + "\n")
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		os.Remove(tmp.Name())
	}
	return err
}
