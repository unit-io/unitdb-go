package unitdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb-go/internal/store"
	"github.com/unit-io/unitdb/server/common"
	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	// Database store
	_ "github.com/unit-io/unitdb-go/internal/db/unitdb"
)

type Client interface {
	// Connect will create a connection to the server.
	Connect() error
	// ConnectContext will create a connection to the server.
	// The context will be used in the grpc stream connection.
	ConnectContext(ctx context.Context) error
	// Disconnect will end the connection with the server, but not before waiting
	// the client wait group is done.
	Disconnect() error
	// DisconnectContext will end the connection with the server, but not before waiting
	// the client wait group is done.
	// The context used grpc stream to signal context done.
	DisconnectContext(ctx context.Context) error
	// TopicFilter is used to receive filtered messages on specififc topic.
	TopicFilter(subTopic string) (*TopicFilter, error)
	// Publish will publish a message with the specified DeliveryMode and content
	// to the specified topic.
	Publish(topic string, payload []byte, pubOpts ...PubOptions) Result
	// Relay sends a request to relay messages for one or more topics those are persisted on the server.
	// Provide a MessageHandler to be executed when a message is published on the topic provided,
	// or nil for the default handler.
	Relay(topics []string, relOpts ...RelOptions) Result
	// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
	// a message is published on the topic provided, or nil for the default handler.
	Subscribe(topic string, subOpts ...SubOptions) Result
	// SubscribeMultiple starts a new subscription for multiple topics. Provide a MessageHandler to be executed when
	// a message is published on the topic provided, or nil for the default handler.
	SubscribeMultiple(subs []string, subOpts ...SubOptions) Result
	// Unsubscribe will end the subscription from each of the topics provided.
	// Messages published to those topics from other clients will no longer be
	// received.
	Unsubscribe(topics ...string) Result
}

type client struct {
	opts       *options
	context    context.Context    // context for the client
	cancel     context.CancelFunc // cancellation function
	messageIds                    // local identifier of messages
	connID     int32              // The unique id of the connection.
	sessID     uint32
	// readIdleTimeout is the read deadline of the connection, see readIdleTimeout.
	readIdleTimeout time.Duration
	epoch           uint32   // The session ID of the connection.
	conn            net.Conn // the network connection
	send            chan *MessageAndResult
	pub             chan *utp.Publish
	notifier        *notifier

	// Time when the keepalive session was last refreshed.
	lastTouched atomic.Value
	// Time when the session received any packer from client.
	lastAction atomic.Value

	// Batch
	batchManager *batchManager

	// idBase maps local message ids to the ids on the wire. It is the first
	// connection's id and does not change when the client reconnects, so
	// that the server's answers to resent messages find their results.
	idBase int32

	// The current connection: its generation, and done, closed when it ends.
	// A loop of an ended connection cannot end its successor.
	connMu   sync.Mutex
	connGen  uint64
	connDone chan struct{}
	connCtx  context.Context // bounds every connection of the client
	started  sync.Once       // starts what outlives a connection

	// subs are the topics the client subscribed to, to subscribe to again
	// when it reconnects.
	subsMu sync.Mutex
	subs   map[string]*utp.Subscription

	// Close.
	closeC chan struct{}
	closeW sync.WaitGroup
	closed uint32
}

func NewClient(target, clientID string, opts ...Options) (Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		opts:       new(options),
		context:    ctx,
		cancel:     cancel,
		messageIds: messageIds{index: make(map[MID]Result), resumedIds: make(map[MID]struct{})},
		send:       make(chan *MessageAndResult, 1), // buffered
		pub:        make(chan *utp.Publish),
		notifier:   newNotifier(100), // Notifier with Queue size 100
		// close
		closeC: make(chan struct{}),
	}
	WithDefaultOptions().set(c.opts)
	for _, opt := range opts {
		opt.set(c.opts)
	}
	// set default options
	c.opts.addServer(target)
	c.opts.setClientID(clientID)

	// Open database connection
	if err := c.openStore(); err != nil {
		return nil, err
	}

	return c, nil
}

// openStore opens the client's message store.
func (c *client) openStore() error {
	path := c.opts.storePath
	if c.opts.clientID != "" {
		path = path + "/" + c.opts.clientID
	}
	return store.Open(path, int64(c.opts.storeSize), false)
}

func StreamConn(
	stream grpc.ClientStream,
) *common.Conn {
	packetFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pbx.Packet).Data
	}
	return &common.Conn{
		Stream: stream,
		InMsg:  &pbx.Packet{},
		OutMsg: &pbx.Packet{},
		Encode: common.Encode(packetFunc),
		Decode: common.Decode(packetFunc),
	}
}

// grpcConn is a connection over a grpc stream that owns its grpc client
// connection: closing the stream alone would leave the client connection and
// its goroutines running.
type grpcConn struct {
	*common.Conn
	cc *grpc.ClientConn
}

func (c *grpcConn) Close() error {
	c.Conn.Close()
	return c.cc.Close()
}

func (c *client) close() error {
	if c.conn != nil {
		defer c.conn.Close()
	}

	if !c.setClosed() {
		return errors.New("error disconnecting client")
	}
	c.connMu.Lock()
	gen := c.connGen
	c.connMu.Unlock()
	c.endConnection(gen)

	if c.cancel != nil {
		c.cancel()
	}

	// Signal all goroutines.
	close(c.closeC)

	// Wait for all goroutines to exit.
	c.closeW.Wait()
	// The send and pub channels are left open: goroutines that may still
	// send on them give up on closeC instead.

	c.batchManager.close()

	c.notifier.close()
	store.Close()

	return nil
}

// Connect will create a connection to the server
func (c *client) Connect() error {
	return c.ConnectContext(c.context)
}

// ConnectContext will create a connection to the server
// The context will be used in the grpc stream connection
func (c *client) ConnectContext(ctx context.Context) error {
	// Connect to the server
	if len(c.opts.servers) == 0 {
		return errors.New("no servers defined to connect to")
	}

	// A failed connect closes the store, so open it again on a retry.
	if !store.IsOpen() {
		if err := c.openStore(); err != nil {
			return err
		}
	}

	c.readIdleTimeout = readIdleTimeout

	// ctx bounds the whole connection; the connect timeout only bounds dialing.
	ctx, cancel := context.WithCancel(ctx)
	if err := c.attemptConnection(ctx, c.opts.cleanSession); err != nil {
		cancel()
		// Release the process wide store so that a new client can be created.
		store.Close()
		return err
	}
	clientCancel := c.cancel
	c.cancel = func() {
		cancel()
		if clientCancel != nil {
			clientCancel()
		}
	}

	// Resolve the session before the loops start, so that every message of
	// this connection is logged under it.
	var sessKey uint32
	if c.opts.sessionKey != 0 {
		sessKey = c.opts.sessionKey
	} else {
		sessKey = c.epoch
	}
	resume := false
	if rawSess, err := store.Session.Get(uint64(sessKey)); err == nil && len(rawSess) >= 4 {
		c.sessID = binary.LittleEndian.Uint32(rawSess[:4])
		if c.opts.cleanSession {
			store.Log.Reset(c.sessID)
		} else {
			resume = true
		}
	}
	rawSess := make([]byte, 4)
	binary.LittleEndian.PutUint32(rawSess[0:4], c.sessID)
	store.Session.Put(uint64(sessKey), rawSess)
	if c.epoch != sessKey {
		store.Session.Put(uint64(c.epoch), rawSess)
	}

	c.connCtx = ctx
	c.started.Do(func() {
		// batch manager
		c.newBatchManager(&batchOptions{
			batchDuration:       c.opts.batchDuration,
			batchCountThreshold: c.opts.batchCountThreshold,
			batchByteThreshold:  c.opts.batchByteThreshold,
		})
		go c.dispatcher(ctx) // dispatch messages to client
	})
	c.startConnection(ctx)

	// Resend what the session left unfinished, now that the loops run.
	if resume {
		c.resume(c.sessID, c.opts.resumeSubs)
	}
	if c.opts.connectionHandler != nil {
		go c.opts.connectionHandler(c)
	}

	return nil
}

// startConnection starts the loops of the connection just made.
func (c *client) startConnection(ctx context.Context) {
	c.connMu.Lock()
	c.connGen++
	gen, conn := c.connGen, c.conn
	done := make(chan struct{})
	c.connDone = done
	c.connMu.Unlock()

	if c.opts.keepAlive != 0 {
		c.updateLastAction()
		c.updateLastTouched()
		go c.keepalive(ctx, gen, done)
	}
	go c.readLoop(ctx, gen, conn, done)  // process incoming messages
	go c.writeLoop(ctx, gen, conn, done) // send messages to servers
}

// endConnection stops the loops of connection gen, if it is the current one
// and still running, and reports whether it did.
func (c *client) endConnection(gen uint64) (net.Conn, bool) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if gen != c.connGen || c.connDone == nil {
		return nil, false
	}
	close(c.connDone)
	c.connDone = nil
	return c.conn, true
}

// connected reports whether the client has a live connection.
func (c *client) connected() bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.connDone != nil
}

// connLost ends connection gen after err, once: it reconnects if the client
// reconnects by itself, and closes the client otherwise.
func (c *client) connLost(gen uint64, err error) {
	conn, ok := c.endConnection(gen)
	if !ok || c.isClosed() {
		return
	}
	if conn != nil {
		conn.Close()
	}
	if c.opts.connectionLostHandler != nil {
		go c.opts.connectionLostHandler(c, err)
	}
	if c.opts.autoReconnect && c.connCtx != nil && c.connCtx.Err() == nil {
		go c.reconnect()
		return
	}
	c.close()
}

// reconnect connects again, trying each server in turn with a growing pause
// between attempts, until it succeeds or the client closes. It resumes the
// session, resends what the server had not acknowledged, and subscribes again.
func (c *client) reconnect() {
	pause := 100 * time.Millisecond
	for {
		select {
		case <-c.closeC:
			return
		case <-c.connCtx.Done():
			c.close()
			return
		case <-time.After(pause):
		}
		// The session goes on: never ask for a clean one again.
		if err := c.attemptConnection(c.connCtx, false); err == nil {
			if c.isClosed() {
				c.conn.Close()
				return
			}
			c.startConnection(c.connCtx)
			c.resume(c.sessID, c.opts.resumeSubs)
			c.resubscribe()
			if c.opts.connectionHandler != nil {
				go c.opts.connectionHandler(c)
			}
			return
		}
		if pause *= 2; pause > c.opts.maxReconnectInterval {
			pause = c.opts.maxReconnectInterval
		}
	}
}

// trackSubscriptions records subscriptions, to subscribe again after a
// reconnect.
func (c *client) trackSubscriptions(subs []*utp.Subscription) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if c.subs == nil {
		c.subs = make(map[string]*utp.Subscription)
	}
	for _, sub := range subs {
		c.subs[sub.Topic] = sub
	}
}

// untrackSubscriptions forgets topics the client unsubscribed from.
func (c *client) untrackSubscriptions(topics []string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for _, text := range topics {
		t := new(topic)
		if t.parse(text) {
			delete(c.subs, t.wire())
		}
		delete(c.subs, text)
	}
}

// resubscribe subscribes again to the tracked topics: the server drops a
// connection's subscriptions when it ends.
func (c *client) resubscribe() {
	c.subsMu.Lock()
	subMsg := &utp.Subscribe{}
	for _, sub := range c.subs {
		subMsg.Subscriptions = append(subMsg.Subscriptions, sub)
	}
	c.subsMu.Unlock()
	if len(subMsg.Subscriptions) == 0 {
		return
	}
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	subMsg.MessageID = c.outboundID(c.nextID(r))
	c.sendMessage(&MessageAndResult{m: subMsg, r: r})
}

// attemptConnection connects to the first server that accepts the connection.
func (c *client) attemptConnection(ctx context.Context, clean bool) error {
	var err error
	for _, uri := range c.opts.servers {
		var conn net.Conn
		if conn, err = c.dial(ctx, uri); err != nil {
			continue
		}

		// get Connect message from options.
		cm := newConnectMsgFromOptions(c.opts, uri)
		cm.CleanSessFlag = clean
		rc, epoch, connID, cerr := Connect(conn, cm)
		if cerr == nil && rc == utp.Accepted {
			c.connMu.Lock()
			c.conn = conn
			c.connMu.Unlock()
			c.epoch = uint32(epoch)
			c.connID = connID
			if c.idBase == 0 {
				// The first connection: its id is the session's id, until
				// the session store names an older one, and the id base.
				c.idBase = connID
				c.sessID = uint32(connID)
				c.messageIds.reset(MID(connID))
			}
			return nil
		}
		conn.Close()
		if cerr != nil {
			err = cerr
		} else {
			err = fmt.Errorf("connection to %s refused, return code %d", uri.Host, rc)
		}
	}
	return err
}

// dial opens a network connection to the server. ctx bounds the life of a
// grpc stream; the connect timeout bounds dialing.
func (c *client) dial(ctx context.Context, uri *url.URL) (net.Conn, error) {
	switch uri.Scheme {
	case "grpc", "ws":
		dialCtx, dialCancel := ctx, context.CancelFunc(func() {})
		if c.opts.connectTimeout > 0 {
			dialCtx, dialCancel = context.WithTimeout(ctx, c.opts.connectTimeout)
		}
		// passthrough hands the address to the dialer as it is, as
		// grpc.Dial did.
		conn, err := grpc.NewClient(
			"passthrough:///"+uri.Host,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// A frame of the largest size the server accepts, with its headers.
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(lp.MaxFrameSize+1<<10)),
		)
		if err != nil {
			dialCancel()
			return nil, err
		}
		err = waitReady(dialCtx, conn)
		dialCancel()
		if err != nil {
			conn.Close()
			return nil, err
		}

		// Connect to grpc stream. The stream lives as long as the connection.
		stream, err := pbx.NewUnitdbClient(conn).Stream(ctx)
		if err != nil {
			conn.Close()
			return nil, err
		}
		return &grpcConn{Conn: StreamConn(stream), cc: conn}, nil
	case "tcp", "unix":
		return net.DialTimeout(uri.Scheme, uri.Host, c.opts.connectTimeout)
	default:
		return nil, fmt.Errorf("unsupported server scheme %q", uri.Scheme)
	}
}

// waitReady connects conn and waits until it is ready or ctx is done, as the
// deprecated grpc.WithBlock did: a server that does not answer fails the
// dial, and the next server is tried.
func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

// Disconnect will disconnect the connection to the server
func (c *client) Disconnect() error {
	return c.DisconnectContext(c.context)
}

// Disconnect will disconnect the connection to the server
func (c *client) DisconnectContext(ctx context.Context) error {
	if err := c.ok(); err != nil {
		// Disconnect() called but not connected
		return nil
	}

	// A client that never connected, or is reconnecting, has nothing to tell
	// the server.
	if c.conn == nil || !c.connected() {
		return c.close()
	}

	defer c.close()
	m := &utp.Disconnect{}
	r := &DisconnectResult{result: result{complete: make(chan struct{})}}
	if !c.sendMessage(&MessageAndResult{m: m, r: r}) {
		return nil
	}
	_, err := r.Get(ctx, c.opts.writeTimeout)
	return err
}

// internalConnLost ends the current connection after err.
func (c *client) internalConnLost(err error) {
	c.connMu.Lock()
	gen := c.connGen
	c.connMu.Unlock()
	c.connLost(gen, err)
}

// serverDisconnect ends the current connection when the server disconnects.
func (c *client) serverDisconnect(err error) {
	c.internalConnLost(err)
}

func (c *client) TopicFilter(subscriptionTopic string) (*TopicFilter, error) {
	topic := new(topic)
	topic.parse(subscriptionTopic)
	if err := topic.validate(validateMinLength,
		validateMaxLenth,
		validateMaxDepth,
		validateTopicParts); err != nil {
		return nil, err
	}
	t := &TopicFilter{subscriptionTopic: topic, updates: make(chan []*PubMessage)}
	c.notifier.addFilter(t.filter)

	return t, nil
}

// Publish will publish a message with the specified DeliveryMode and content
// to the specified topic.
func (c *client) Publish(pubTopic string, payload []byte, pubOpts ...PubOptions) Result {
	r := &PublishResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(err)
		return r
	}

	opts := new(pubOptions)
	for _, opt := range pubOpts {
		opt.set(opts)
	}

	deliveryMode := opts.deliveryMode
	delay := opts.delay
	ttl := opts.ttl
	t := new(topic)

	// parse the topic.
	if ok := t.parse(pubTopic); !ok {
		r.setError(errors.New("publish: unable to parse topic"))
		return r
	}

	if err := t.validate(validateMinLength,
		validateMaxLenth,
		validateMaxDepth); err != nil {
		r.setError(err)
		return r
	}

	if dMode, ok := t.getOption("delivery_mode"); ok {
		val, err := strconv.ParseInt(dMode, 10, 64)
		if err == nil {
			deliveryMode = uint8(val)
		}
	}

	if d, ok := t.getOption("delay"); ok {
		val, err := strconv.ParseInt(d, 10, 64)
		if err == nil {
			delay = int32(val)
		}
	}

	if dur, ok := t.getOption("ttl"); ok {
		ttl = dur
	}

	pubMsg := &utp.PublishMessage{
		Topic:   t.wire(),
		Payload: payload,
		Ttl:     ttl,
	}

	// Check batch or delay delivery.
	if deliveryMode == 2 || delay > 0 {
		return c.batchManager.add(delay, pubMsg)
	}
	pub := &utp.Publish{DeliveryMode: deliveryMode, Messages: []*utp.PublishMessage{pubMsg}}

	if pub.MessageID == 0 {
		mID := c.nextID(r)
		pub.MessageID = c.outboundID(mID)
	}

	publishWaitTimeout := c.opts.writeTimeout
	if publishWaitTimeout == 0 {
		publishWaitTimeout = time.Second * 30
	}

	// persist outbound
	c.storeOutbound(pub)

	select {
	case c.send <- &MessageAndResult{m: pub, r: r}:
	case <-c.closeC:
		r.setError(errClientClosed)
		return r
	case <-time.After(publishWaitTimeout):
		r.setError(errors.New("publish timeout error occurred"))
		return r
	}

	return r
}

// Relay send a new relay request. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (c *client) Relay(topics []string, relOpts ...RelOptions) Result {
	r := &RelayResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(err)
		return r
	}

	opts := new(relOptions)
	for _, opt := range relOpts {
		opt.set(opts)
	}

	relMsg := &utp.Relay{}

	for _, relTopic := range topics {
		last := opts.last
		t := new(topic)

		// parse the topic.
		if ok := t.parse(relTopic); !ok {
			r.setError(errors.New("relay: unable to parse topic"))
			return r
		}

		if err := t.validate(validateMinLength,
			validateMaxLenth,
			validateMaxDepth,
			validateMultiWildcard,
			validateTopicParts); err != nil {
			r.setError(err)
			return r
		}

		if dur, ok := t.getOption("last"); ok {
			last = dur
		}

		relMsg.RelayRequests = append(relMsg.RelayRequests, &utp.RelayRequest{Topic: t.wire(), Last: last})
	}

	if relMsg.MessageID == 0 {
		mID := c.nextID(r)
		relMsg.MessageID = c.outboundID(mID)
	}

	relayWaitTimeout := c.opts.writeTimeout
	if relayWaitTimeout == 0 {
		relayWaitTimeout = time.Second * 30
	}

	// persist outbound
	c.storeOutbound(relMsg)

	select {
	case c.send <- &MessageAndResult{m: relMsg, r: r}:
	case <-c.closeC:
		r.setError(errClientClosed)
		return r
	case <-time.After(relayWaitTimeout):
		r.setError(errors.New("relay request timeout error occurred"))
		return r
	}

	return r
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (c *client) Subscribe(subTopic string, subOpts ...SubOptions) Result {
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(err)
		return r
	}

	opts := new(subOptions)
	for _, opt := range subOpts {
		opt.set(opts)
	}

	subMsg := &utp.Subscribe{}

	deliveryMode := opts.deliveryMode
	delay := opts.delay
	t := new(topic)

	// parse the topic.
	if ok := t.parse(subTopic); !ok {
		r.setError(errors.New("subscribe: unable to parse topic"))
		return r
	}

	if err := t.validate(validateMinLength,
		validateMaxLenth,
		validateMaxDepth,
		validateMultiWildcard,
		validateTopicParts); err != nil {
		r.setError(err)
		return r
	}

	if dMode, ok := t.getOption("delivery_mode"); ok {
		val, err := strconv.ParseInt(dMode, 10, 64)
		if err == nil {
			deliveryMode = uint8(val)
		}
	}

	if d, ok := t.getOption("delay"); ok {
		val, err := strconv.ParseInt(d, 10, 64)
		if err == nil {
			delay = int32(val)
		}
	}

	subMsg.Subscriptions = append(subMsg.Subscriptions, &utp.Subscription{DeliveryMode: deliveryMode, Delay: delay, Topic: t.wire()})
	c.trackSubscriptions(subMsg.Subscriptions)

	if subMsg.MessageID == 0 {
		mID := c.nextID(r)
		subMsg.MessageID = c.outboundID(mID)
	}

	subscribeWaitTimeout := c.opts.writeTimeout
	if subscribeWaitTimeout == 0 {
		subscribeWaitTimeout = time.Second * 30
	}

	// persist outbound
	c.storeOutbound(subMsg)

	select {
	case c.send <- &MessageAndResult{m: subMsg, r: r}:
	case <-c.closeC:
		r.setError(errClientClosed)
		return r
	case <-time.After(subscribeWaitTimeout):
		r.setError(errors.New("subscribe timeout error occurred"))
		return r
	}

	return r
}

// SubscribeMultiple starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (c *client) SubscribeMultiple(topics []string, subOpts ...SubOptions) Result {
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(err)
		return r
	}

	opts := new(subOptions)
	for _, opt := range subOpts {
		opt.set(opts)
	}

	subMsg := &utp.Subscribe{}
	for _, subTopic := range topics {
		deliveryMode := opts.deliveryMode
		delay := opts.delay
		t := new(topic)

		// parse the topic.
		if ok := t.parse(subTopic); !ok {
			r.setError(errors.New("SubscribeMultiple: unable to parse topic"))
			return r
		}

		if err := t.validate(validateMinLength,
			validateMaxLenth,
			validateMaxDepth,
			validateMultiWildcard,
			validateTopicParts); err != nil {
			r.setError(err)
			return r
		}

		if dMode, ok := t.getOption("delivery_mode"); ok {
			val, err := strconv.ParseInt(dMode, 10, 64)
			if err == nil {
				deliveryMode = uint8(val)
			}
		}

		if d, ok := t.getOption("delay"); ok {
			val, err := strconv.ParseInt(d, 10, 64)
			if err == nil {
				delay = int32(val)
			}
		}

		subMsg.Subscriptions = append(subMsg.Subscriptions, &utp.Subscription{DeliveryMode: deliveryMode, Delay: delay, Topic: t.wire()})
	}
	c.trackSubscriptions(subMsg.Subscriptions)

	if subMsg.MessageID == 0 {
		mID := c.nextID(r)
		subMsg.MessageID = c.outboundID(mID)
	}

	subscribeWaitTimeout := c.opts.writeTimeout
	if subscribeWaitTimeout == 0 {
		subscribeWaitTimeout = time.Second * 30
	}

	// persist outbound
	c.storeOutbound(subMsg)

	select {
	case c.send <- &MessageAndResult{m: subMsg, r: r}:
	case <-c.closeC:
		r.setError(errClientClosed)
		return r
	case <-time.After(subscribeWaitTimeout):
		r.setError(errors.New("subscribe timeout error occurred"))
		return r
	}

	return r
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (c *client) Unsubscribe(topics ...string) Result {
	r := &UnsubscribeResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(err)
		return r
	}
	unsubMsg := &utp.Unsubscribe{}
	var subs []*utp.Subscription
	for _, topic := range topics {
		sub := &utp.Subscription{Topic: topic}
		subs = append(subs, sub)
	}
	unsubMsg.Subscriptions = subs
	c.untrackSubscriptions(topics)

	if unsubMsg.MessageID == 0 {
		mID := c.nextID(r)
		unsubMsg.MessageID = c.outboundID(mID)
	}

	unsubscribeWaitTimeout := c.opts.writeTimeout
	if unsubscribeWaitTimeout == 0 {
		unsubscribeWaitTimeout = time.Second * 30
	}

	// persist outbound
	c.storeOutbound(unsubMsg)

	select {
	case c.send <- &MessageAndResult{m: unsubMsg, r: r}:
	case <-c.closeC:
		r.setError(errClientClosed)
		return r
	case <-time.After(unsubscribeWaitTimeout):
		r.setError(errors.New("unsubscribe timeout error occurred"))
		return r
	}

	return r
}

// resume resends what a session left unfinished, so that delivery is ensured
// even after an application crash.
func (c *client) resume(prefix uint32, subscription bool) {
	keys := store.Log.Keys(prefix)
	for _, k := range keys {
		msg := store.Log.Get(k)
		if msg == nil {
			continue
		}

		if store.IsInboundKey(k) {
			// A message of the server that the client has not finished receiving.
			switch m := msg.(type) {
			case *utp.Publish:
				// Received but not yet receipted: ask for it again.
				c.sendMessage(&MessageAndResult{m: &utp.ControlMessage{MessageID: m.MessageID, MessageType: utp.PUBLISH, FlowControl: utp.RECEIVE}})
			case *utp.ControlMessage:
				switch m.FlowControl {
				case utp.NOTIFY:
					c.sendMessage(&MessageAndResult{m: &utp.ControlMessage{MessageID: m.MessageID, MessageType: utp.PUBLISH, FlowControl: utp.RECEIVE}})
				case utp.RECEIPT:
					c.sendMessage(&MessageAndResult{m: m})
				default:
					store.Log.Delete(k)
				}
			default:
				store.Log.Delete(k)
			}
			continue
		}

		// A message of the client that the server has not acknowledged.
		switch msg.Type() {
		case utp.RELAY:
			relMsg := msg.(*utp.Relay)
			r := &RelayResult{result: result{complete: make(chan struct{})}}
			r.messageID = msg.Info().MessageID
			c.messageIds.resumeID(MID(r.messageID))
			r.reqs = relMsg.RelayRequests
			c.sendMessage(&MessageAndResult{m: msg, r: r})
		case utp.SUBSCRIBE:
			if subscription {
				subMsg := msg.(*utp.Subscribe)
				r := &SubscribeResult{result: result{complete: make(chan struct{})}}
				r.messageID = msg.Info().MessageID
				c.messageIds.resumeID(MID(r.messageID))
				r.subs = subMsg.Subscriptions
				c.sendMessage(&MessageAndResult{m: msg, r: r})
			}
		case utp.UNSUBSCRIBE:
			if subscription {
				r := &UnsubscribeResult{result: result{complete: make(chan struct{})}}
				c.messageIds.resumeID(MID(msg.Info().MessageID))
				c.sendMessage(&MessageAndResult{m: msg, r: r})
			}
		case utp.PUBLISH:
			r := &PublishResult{result: result{complete: make(chan struct{})}}
			r.messageID = msg.Info().MessageID
			c.messageIds.resumeID(MID(r.messageID))
			c.sendMessage(&MessageAndResult{m: msg, r: r})
		default:
			store.Log.Delete(k)
		}
	}
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}

func (c *client) inboundID(id uint16) MID {
	return MID(c.idBase - int32(id))
}

func (c *client) outboundID(mid MID) (id uint16) {
	return uint16(c.idBase - (int32(mid)))
}

func (c *client) updateLastAction() {
	if c.opts.keepAlive != 0 {
		c.lastAction.Store(TimeNow())
	}
}

func (c *client) updateLastTouched() {
	c.lastTouched.Store(TimeNow())
}

// sendMessage queues m for the write loop, unless the client closes first.
func (c *client) sendMessage(m *MessageAndResult) bool {
	select {
	case c.send <- m:
		return true
	case <-c.closeC:
		return false
	}
}

// The store is closed with the client, so a closed client stores nothing.
func (c *client) storeInbound(m lp.MessagePack) {
	if c.isClosed() {
		return
	}
	store.Log.PersistInbound(uint32(c.sessID), m)
}

func (c *client) storeOutbound(m lp.MessagePack) {
	if c.isClosed() {
		return
	}
	store.Log.PersistOutbound(uint32(c.sessID), m)
}

// Set closed flag; return true if not already closed.
func (c *client) setClosed() bool {
	return atomic.CompareAndSwapUint32(&c.closed, 0, 1)
}

// Check whether connection was closed.
func (c *client) isClosed() bool {
	return atomic.LoadUint32(&c.closed) != 0
}

// Check read ok status.
var errClientClosed = errors.New("client connection is closed")

func (c *client) ok() error {
	if c.isClosed() {
		return errClientClosed
	}
	return nil
}
