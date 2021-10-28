package unitdb

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb-go/internal/store"
	"github.com/unit-io/unitdb/server/common"
	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
	"google.golang.org/grpc"

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
	epoch      uint32   // The session ID of the connection.
	conn       net.Conn // the network connection
	send       chan *MessageAndResult
	recv       chan lp.MessagePack
	pub        chan *utp.Publish
	notifier   *notifier

	// Time when the keepalive session was last refreshed.
	lastTouched atomic.Value
	// Time when the session received any packer from client.
	lastAction atomic.Value

	// Batch
	batchManager *batchManager

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
		recv:       make(chan lp.MessagePack),
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
	path := c.opts.storePath
	if clientID != "" {
		path = path + "/" + clientID
	}
	if err := store.Open(path, int64(c.opts.storeSize), false); err != nil {
		return nil, err
	}

	return c, nil
}

func StreamConn(
	stream grpc.Stream,
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

func (c *client) close() error {
	defer c.conn.Close()

	if !c.setClosed() {
		return errors.New("error disconnecting client")
	}

	if c.cancel != nil {
		c.cancel()
	}

	// Signal all goroutines.
	close(c.closeC)

	// Wait for all goroutines to exit.
	c.closeW.Wait()
	close(c.send)
	// close(c.ack)
	close(c.recv)
	close(c.pub)

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

	ctx, c.cancel = context.WithTimeout(ctx, c.opts.connectTimeout)

	if err := c.attemptConnection(ctx); err != nil {
		return err
	}

	// batch manager
	c.newBatchManager(&batchOptions{
		batchDuration:       c.opts.batchDuration,
		batchCountThreshold: c.opts.batchCountThreshold,
		batchByteThreshold:  c.opts.batchByteThreshold,
	})

	if c.opts.keepAlive != 0 {
		c.updateLastAction()
		c.updateLastTouched()
		go c.keepalive(ctx)
	}
	// c.closeW.Add(3)
	go c.readLoop(ctx)   // process incoming messages
	go c.writeLoop(ctx)  // send messages to servers
	go c.dispatcher(ctx) // dispatch messages to client

	// Take care of any messages in the store
	var sessKey uint32
	if c.opts.sessionKey != 0 {
		sessKey = c.opts.sessionKey
	} else {
		sessKey = c.epoch
	}

	sessID := c.connID
	if rawSess, err := store.Session.Get(uint64(sessKey)); err == nil {
		sessID := binary.LittleEndian.Uint32(rawSess[:4])
		if !c.opts.cleanSession {
			c.resume(sessID, c.opts.resumeSubs)
		} else {
			store.Log.Reset(sessID)
		}
		c.sessID = sessID
	}
	rawSess := make([]byte, 4)
	binary.LittleEndian.PutUint32(rawSess[0:4], uint32(sessID))
	store.Session.Put(uint64(sessKey), rawSess)
	if c.epoch != sessKey {
		store.Session.Put(uint64(c.epoch), rawSess)
	}

	return nil
}

func (c *client) attemptConnection(ctx context.Context) (err error) {
	for _, uri := range c.opts.servers {
		switch uri.Scheme {
		case "grpc", "ws":
			conn, err := grpc.DialContext(
				ctx,
				uri.Host,
				grpc.WithBlock(),
				grpc.WithInsecure(),
				grpc.WithTimeout(c.opts.connectTimeout),
			)
			if err != nil {
				return err
			}

			// Connect to grpc stream
			stream, err := pbx.NewUnitdbClient(conn).Stream(ctx)
			if err != nil {
				log.Fatal(err)
			}
			c.conn = StreamConn(stream)
		case "tcp":
			conn, err := net.DialTimeout("tcp", uri.Host, c.opts.connectTimeout)
			if err != nil {
				return err
			}
			c.conn = conn
		case "unix":
			conn, err := net.DialTimeout("unix", uri.Host, c.opts.connectTimeout)
			if err != nil {
				return err
			}
			c.conn = conn
		}

		// get Connect message from options.
		cm := newConnectMsgFromOptions(c.opts, uri)
		rc, epoch, connId, err1 := Connect(c.conn, cm)
		if rc == utp.Accepted {
			c.epoch = uint32(epoch)
			c.connID = connId
			c.sessID = uint32(connId)
			c.messageIds.reset(MID(c.connID))
			break // successfully connected
		}
		if c.conn != nil {
			c.DisconnectContext(ctx)
		}
		err = err1
	}
	return err
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

	defer c.close()
	m := &utp.Disconnect{}
	r := &DisconnectResult{result: result{complete: make(chan struct{})}}
	c.send <- &MessageAndResult{m: m, r: r}
	_, err := r.Get(ctx, c.opts.writeTimeout)
	return err
}

// internalConnLost cleanup when connection is lost or an error occurs
func (c *client) internalConnLost(err error) {
	// It is possible that internalConnLost will be called multiple times simultaneously
	// (including after sending a DisconnectMessage) as such we only do cleanup etc if the
	// routines were actually running and are not being disconnected at users request
	if err := c.ok(); err == nil {
		if c.opts.connectionLostHandler != nil {
			go c.opts.connectionLostHandler(c, err)
		}
		c.close()
	}
}

// serverDisconnect cleanup when server send disconnect request or an error occurs.
func (c *client) serverDisconnect(err error) {
	if err := c.ok(); err == nil {
		if c.opts.connectionLostHandler != nil {
			go c.opts.connectionLostHandler(c, err)
		}
		c.close()
	}
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
		Topic:   t.topic,
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

		relMsg.RelayRequests = append(relMsg.RelayRequests, &utp.RelayRequest{Topic: t.topic, Last: last})
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

	subMsg.Subscriptions = append(subMsg.Subscriptions, &utp.Subscription{DeliveryMode: deliveryMode, Delay: delay, Topic: t.topic})

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

		subMsg.Subscriptions = append(subMsg.Subscriptions, &utp.Subscription{DeliveryMode: deliveryMode, Delay: delay, Topic: t.topic})
	}

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
	case <-time.After(unsubscribeWaitTimeout):
		r.setError(errors.New("unsubscribe timeout error occurred"))
		return r
	}

	return r
}

// Load all stored messages and resend them to ensure DeliveryMode even after an application crash.
func (c *client) resume(prefix uint32, subscription bool) {
	keys := store.Log.Keys(prefix)
	for _, k := range keys {
		msg := store.Log.Get(k)
		if msg == nil {
			continue
		}
		// isKeyOutbound
		if (k & (1 << 4)) == 0 {
			switch msg.Type() {
			case utp.RELAY:
				relMsg := msg.(*utp.Relay)
				r := &RelayResult{result: result{complete: make(chan struct{})}}
				r.messageID = msg.Info().MessageID
				c.messageIds.resumeID(MID(r.messageID))
				r.reqs = relMsg.RelayRequests
				c.send <- &MessageAndResult{m: msg, r: r}
			case utp.SUBSCRIBE:
				if subscription {
					subMsg := msg.(*utp.Subscribe)
					r := &SubscribeResult{result: result{complete: make(chan struct{})}}
					r.messageID = msg.Info().MessageID
					c.messageIds.resumeID(MID(r.messageID))
					r.subs = subMsg.Subscriptions
					c.send <- &MessageAndResult{m: msg, r: r}
				}
			case utp.UNSUBSCRIBE:
				if subscription {
					r := &UnsubscribeResult{result: result{complete: make(chan struct{})}}
					c.messageIds.resumeID(MID(r.messageID))
					c.send <- &MessageAndResult{m: msg, r: r}
				}

			case utp.PUBLISH:
				r := &PublishResult{result: result{complete: make(chan struct{})}}
				r.messageID = msg.Info().MessageID
				c.messageIds.resumeID(MID(r.messageID))
				c.send <- &MessageAndResult{m: msg, r: r}
			case utp.FLOWCONTROL:
				ctrlMsg := msg.(*utp.ControlMessage)
				switch ctrlMsg.FlowControl {
				case utp.RECEIPT:
					c.send <- &MessageAndResult{m: ctrlMsg}
				}
			default:
				store.Log.Delete(k)
			}
		} else {
			switch msg.Type() {
			case utp.FLOWCONTROL:
				ctrlMsg := msg.(*utp.ControlMessage)
				c.messageIds.resumeID(MID(ctrlMsg.MessageID))
				switch ctrlMsg.FlowControl {
				case utp.NOTIFY:
					c.recv <- msg
				}
			default:
				store.Log.Delete(k)
			}
		}
	}
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}

func (c *client) inboundID(id uint16) MID {
	return MID(c.connID - int32(id))
}

func (c *client) outboundID(mid MID) (id uint16) {
	return uint16(c.connID - (int32(mid)))
}

func (c *client) updateLastAction() {
	if c.opts.keepAlive != 0 {
		c.lastAction.Store(TimeNow())
	}
}

func (c *client) updateLastTouched() {
	c.lastTouched.Store(TimeNow())
}

func (c *client) storeInbound(m lp.MessagePack) {
	store.Log.PersistInbound(uint32(c.sessID), m)
}

func (c *client) storeOutbound(m lp.MessagePack) {
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
func (c *client) ok() error {
	if c.isClosed() {
		return errors.New("client connection is closed")
	}
	return nil
}
