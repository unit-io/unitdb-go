package unitd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/unit-io/unitd-go/packets"
	"github.com/unit-io/unitd-go/store"
	plugins "github.com/unit-io/unitd/plugins/grpc"
	pbx "github.com/unit-io/unitd/proto"
	"google.golang.org/grpc"

	// Database store
	_ "github.com/unit-io/unitd-go/db/unitdb"
)

// Various constant parts of the Client Connection.
const (
	// MasterContract contract is default contract used for topics if client program does not specify Contract in the request
	MasterContract = uint32(3376684800)
)

type Client interface {
	// Connect will create a connection to the server
	Connect() error
	// ConnectContext will create a connection to the server
	// The context will be used in the grpc stream connection
	ConnectContext(ctx context.Context) error
	// Disconnect will end the connection with the server, but not before waiting
	// the client wait group is done.
	Disconnect() error
	// DisconnectContext will end the connection with the server, but not before waiting
	// the client wait group is done.
	// The context used grpc stream to signal context done.
	DisconnectContext(ctx context.Context) error
	// Publish will publish a message with the specified QoS and content
	// to the specified topic.
	Publish(topic, payload []byte, pubOpts ...PubOptions) Result
	// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
	// a message is published on the topic provided, or nil for the default handler
	Subscribe(topic []byte, subOpts ...SubOptions) Result
	// Unsubscribe will end the subscription from each of the topics provided.
	// Messages published to those topics from other clients will no longer be
	// received.
	Unsubscribe(topics ...[]byte) Result
}
type client struct {
	mu         sync.Mutex // mutex for the connection
	opts       *options
	context    context.Context    // context for the client
	cancel     context.CancelFunc // cancellation function
	contract   uint32
	messageIds          // local identifier of messages
	connID     uint32   // Theunique id of the connection.
	conn       net.Conn // the network connection
	stream     grpc.Stream
	send       chan *PacketAndResult
	recv       chan packets.Packet
	pub        chan *packets.Publish
	callbacks  map[uint64]MessageHandler

	// Time when the keepalive session was last refreshed
	lastTouched atomic.Value
	// Time when the session received any packer from client
	lastAction atomic.Value

	// Close.
	closeC chan struct{}
	closeW sync.WaitGroup
	closed uint32
}

func NewClient(target string, clientID string, opts ...Options) (Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		opts:       new(options),
		context:    ctx,
		cancel:     cancel,
		contract:   MasterContract,
		messageIds: messageIds{index: make(map[MID]Result)},
		send:       make(chan *PacketAndResult, 1), // buffered
		recv:       make(chan packets.Packet),
		pub:        make(chan *packets.Publish),
		callbacks:  make(map[uint64]MessageHandler),
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
	c.callbacks[0] = c.opts.defaultMessageHandler

	// Open database connection
	path := c.opts.storePath
	if clientID != "" {
		path = path + "/" + clientID
	}
	if err := store.Open(path, int64(c.opts.storeSize), c.opts.storeLogReleaseDuration); err != nil {
		return nil, err
	}

	// Init message store and recover pending messages from log file if reset is set false

	if err := store.InitMessageStore(c.context, false); err != nil {
		return nil, err
	}

	return c, nil
}

func StreamConn(
	stream grpc.Stream,
) *plugins.Conn {
	packetFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pbx.Packet).Data
	}
	return &plugins.Conn{
		Stream: stream,
		InMsg:  &pbx.Packet{},
		OutMsg: &pbx.Packet{},
		Encode: plugins.Encode(packetFunc),
		Decode: plugins.Decode(packetFunc),
	}
}

func (c *client) close() error {
	defer c.conn.Close()

	if !c.setClosed() {
		return errors.New("error disconnecting client")
	}

	// Signal all goroutines.
	close(c.closeC)
	// time.Sleep(c.opts.WriteTimeout)
	// Wait for all goroutines to exit.
	c.closeW.Wait()
	close(c.send)
	close(c.recv)
	close(c.pub)
	store.Close()
	if c.cancel != nil {
		c.cancel()
	}
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

	// var cancel context.CancelFunc
	if c.opts.connectTimeout != 0 {
		ctx, c.cancel = context.WithTimeout(ctx, c.opts.connectTimeout)
	} else {
		ctx, c.cancel = context.WithCancel(ctx)
	}
	// defer cancel()
	if err := c.attemptConnection(ctx); err != nil {
		return err
	}

	// Take care of any messages in the store
	if !c.opts.cleanSession {
		c.resume(c.opts.resumeSubs)
	} else {
		// contract is used as blockId and key prefix
		store.Log.Reset(c.contract)
	}
	if c.opts.keepAlive != 0 {
		c.updateLastAction()
		c.updateLastTouched()
		go c.keepalive(ctx)
	}
	go c.readLoop(ctx)   // process incoming messages
	go c.writeLoop(ctx)  // send messages to servers
	go c.dispatcher(ctx) // dispatch messages to client

	return nil
}

func (c *client) attemptConnection(ctx context.Context) error {
	for _, url := range c.opts.servers {
		conn, err := grpc.Dial(
			url.Host,
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(c.opts.connectTimeout),
		)
		if err != nil {
			return err
		}

		// Connect to grpc stream
		stream, err := pbx.NewUnitdClient(conn).Stream(ctx)
		if err != nil {
			log.Fatal(err)
		}
		c.conn = StreamConn(stream)

		// get Connect message from options.
		cm := newConnectMsgFromOptions(c.opts, url)
		rc, connId, _ := Connect(c.conn, cm)
		if rc == packets.Accepted {
			c.connID = connId
			c.messageIds.reset(MID(c.connID))
			break // successfully connected
		}
		if c.conn != nil {
			c.DisconnectContext(ctx)
		}
	}
	return nil
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
	p := &packets.Disconnect{}
	r := &DisconnectResult{result: result{complete: make(chan struct{})}}
	c.send <- &PacketAndResult{p: p, r: r}
	_, err := r.Get(ctx, c.opts.writeTimeout)
	return err
}

// internalConnLost cleanup when connection is lost or an error occurs
func (c *client) internalConnLost(err error) {
	// It is possible that internalConnLost will be called multiple times simultaneously
	// (including after sending a DisconnectPacket) as such we only do cleanup etc if the
	// routines were actually running and are not being disconnected at users request
	defer c.close()
	if err := c.ok(); err == nil {
		if c.opts.connectionLostHandler != nil {
			go c.opts.connectionLostHandler(c, err)
		}
	}

	fmt.Println("internalConnLost exiting")
}

// Publish will publish a message with the specified QoS and content
// to the specified topic.
func (c *client) Publish(topic, payload []byte, pubOpts ...PubOptions) Result {
	r := &PublishResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(errors.New("error not connected"))
		return r
	}

	opts := new(pubOptions)
	for _, opt := range pubOpts {
		opt.set(opts)
	}

	fh := packets.FixedHeader{Qos: opts.qos, Retain: opts.retained}
	pub := &packets.Publish{}
	pub.Topic = topic
	pub.Payload = payload
	pub.Qos = opts.qos

	if fh.Qos != 0 && pub.MessageID == 0 {
		mID := c.nextID(r)
		pub.MessageID = c.outboundID(mID)
	}
	publishWaitTimeout := c.opts.writeTimeout
	if publishWaitTimeout == 0 {
		publishWaitTimeout = c.opts.writeTimeout
	}
	// persist outbound
	c.storeOutbound(pub)
	select {
	case c.send <- &PacketAndResult{p: pub, r: r}:
	case <-time.After(publishWaitTimeout):
		r.setError(errors.New("publish timeout error occurred"))
		return r
	}
	return r
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (c *client) Subscribe(topic []byte, subOpts ...SubOptions) Result {
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	if err := c.ok(); err != nil {
		r.setError(errors.New("error not connected"))
		return r
	}
	opts := new(subOptions)
	for _, opt := range subOpts {
		opt.set(opts)
	}

	sub := &packets.Subscribe{}
	sub.Subscribers = append(sub.Subscribers, &packets.Subscriber{Topic: topic, Qos: opts.qos})

	if opts.callback != nil {
	}

	if sub.MessageID == 0 {
		mID := c.nextID(r)
		sub.MessageID = c.outboundID(mID)
	}
	subscribeWaitTimeout := c.opts.writeTimeout
	if subscribeWaitTimeout == 0 {
		subscribeWaitTimeout = time.Second * 30
	}
	// persist outbound
	c.storeOutbound(sub)
	select {
	case c.send <- &PacketAndResult{p: sub, r: r}:
	case <-time.After(subscribeWaitTimeout):
		r.setError(errors.New("subscribe timeout error occurred"))
		return r
	}

	return r
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (c *client) Unsubscribe(topics ...[]byte) Result {
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	unsub := &packets.Unsubscribe{}
	var subs []*packets.Subscriber
	for _, topic := range topics {
		sub := &packets.Subscriber{Topic: make([]byte, len(topic))}
		copy(sub.Topic, topic)
		subs = append(subs, sub)
	}
	unsub.Subscribers = subs
	if unsub.MessageID == 0 {
		mID := c.nextID(r)
		unsub.MessageID = c.outboundID(mID)
	}
	unsubscribeWaitTimeout := c.opts.writeTimeout
	if unsubscribeWaitTimeout == 0 {
		unsubscribeWaitTimeout = time.Second * 30
	}
	// persist outbound
	c.storeOutbound(unsub)
	select {
	case c.send <- &PacketAndResult{p: unsub, r: r}:
	case <-time.After(unsubscribeWaitTimeout):
		r.setError(errors.New("unsubscribe timeout error occurred"))
		return r
	}
	return r
}

// Load all stored messages and resend them to ensure QOS > 1,2 even after an application crash.
func (c *client) resume(subscription bool) {
	// contract is used as blockId and key prefix
	keys := store.Log.Keys(c.contract)
	for _, k := range keys {
		msg := store.Log.Get(k)
		if msg == nil {
			continue
		}
		info := msg.Info()
		// isKeyOutbound
		if (k & (1 << 4)) == 0 {
			switch msg.(type) {
			case *packets.Subscribe:
				if subscription {
					p := msg.(*packets.Subscribe)
					r := &SubscribeResult{result: result{complete: make(chan struct{})}}
					r.messageID = info.MessageID
					var topics []TopicQOSTuple
					for _, sub := range p.Subscribers {
						var t TopicQOSTuple
						t.Topic = sub.Topic
						t.Qos = uint8(sub.Qos)
						topics = append(topics, t)
					}
					r.subs = append(r.subs, topics...)
					//c.claimID(token, details.MessageID)
					c.send <- &PacketAndResult{p: msg, r: r}
				}
			case *packets.Unsubscribe:
				if subscription {
					r := &UnsubscribeResult{result: result{complete: make(chan struct{})}}
					c.send <- &PacketAndResult{p: msg, r: r}
				}

			case *packets.Pubrel:
				c.send <- &PacketAndResult{p: msg, r: nil}
			case *packets.Publish:
				r := &PublishResult{result: result{complete: make(chan struct{})}}
				r.messageID = info.MessageID
				// c.claimID(token, details.MessageID)
				c.send <- &PacketAndResult{p: msg, r: r}
			default:
				store.Log.Delete(k)
			}
		} else {
			switch msg.(type) {
			case *packets.Pubrel:
				c.recv <- msg
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

func (c *client) inboundID(id uint32) MID {
	// return MID(c.connID - ((id << 4) | uint32(1)))
	return MID(c.connID - id)
}

func (c *client) outboundID(mid MID) (id uint32) {
	// return c.connID - ((uint32(mid) << 4) | uint32(0))
	return c.connID - (uint32(mid))
}

func (c *client) updateLastAction() {
	if c.opts.keepAlive != 0 {
		c.lastAction.Store(TimeNow())
	}
}

func (c *client) updateLastTouched() {
	c.lastTouched.Store(TimeNow())
}

func (c *client) storeInbound(m packets.Packet) {
	blockId := uint64(c.contract)
	k := uint64(c.inboundID(m.Info().MessageID))<<32 + blockId
	fmt.Println("inbound: type, key, qos", m.Type(), k, m.Info().Qos)
	store.Log.PersistInbound(blockId, k, m)
}

func (c *client) storeOutbound(m packets.Packet) {
	blockId := uint64(c.contract)
	k := uint64(c.inboundID(m.Info().MessageID))<<32 + blockId
	fmt.Println("outbound: type, key, qos", m.Type(), k, m.Info().Qos)
	store.Log.PersistOutbound(blockId, k, m)
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
		return errors.New("client connection is closed.")
	}
	return nil
}
