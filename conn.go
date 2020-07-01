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

type ClientConn interface {
	// Connect will create a connection to the server
	Connect() error
	// ConnectContext will create a connection to the server
	// The context will be used in the grpc stream connection
	ConnectContext(ctx context.Context) error
	// Disconnect will end the connection with the server, but not before waiting
	// the client wait group is done.
	Disconnect() error
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
type clientConn struct {
	mu         sync.Mutex // mutex for the connection
	opts       *options
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
	closeW sync.WaitGroup
	closed uint32
}

func NewClient(target string, clientID string, opts ...Options) (ClientConn, error) {
	cc := &clientConn{
		opts:       new(options),
		contract:   MasterContract,
		messageIds: messageIds{index: make(map[MID]Result)},
		send:       make(chan *PacketAndResult, 1), // buffered
		recv:       make(chan packets.Packet),
		pub:        make(chan *packets.Publish),
		callbacks:  make(map[uint64]MessageHandler),
		// Close
		// closeC: make(chan struct{}),
	}
	WithDefaultOptions().set(cc.opts)
	for _, opt := range opts {
		opt.set(cc.opts)
	}
	// set default options
	cc.opts.addServer(target)
	cc.opts.setClientID(clientID)
	cc.callbacks[0] = cc.opts.DefaultMessageHandler

	// Open database connection
	if err := store.Open(cc.opts.StorePath); err != nil {
		return nil, err
	}

	// Init message store and recover pending messages from log file if reset is set false
	if err := store.InitDb(cc.opts.StorePath, int64(cc.opts.StoreSize), cc.opts.StoreLogReleaseDuration, false); err != nil {
		return nil, err
	}

	return cc, nil
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

func (cc *clientConn) close() error {
	defer cc.conn.Close()

	if !cc.setClosed() {
		return errors.New("error disconnecting client")
	}
	if cc.cancel != nil {
		cc.cancel()
	}

	// Wait for all goroutines to exit.
	cc.closeW.Wait()
	close(cc.send)
	close(cc.recv)
	close(cc.pub)
	store.Close()
	return nil
}

// Connect will create a connection to the server
func (cc *clientConn) Connect() error {
	return cc.ConnectContext(context.Background())
}

// ConnectContext will create a connection to the server
// The context will be used in the grpc stream connection
func (cc *clientConn) ConnectContext(ctx context.Context) error {
	// Connect to the server
	if len(cc.opts.Servers) == 0 {
		return errors.New("no servers defined to connect to")
	}

	if cc.opts.ConnectTimeout != 0 {
		ctx, cc.cancel = context.WithTimeout(ctx, cc.opts.ConnectTimeout)
	} else {
		ctx, cc.cancel = context.WithCancel(ctx)
	}
	if err := cc.attemptConnection(ctx); err != nil {
		return err
	}

	// Take care of any messages in the store
	if !cc.opts.CleanSession {
		cc.resume(cc.opts.ResumeSubs)
	} else {
		// contract is used as blockId and key prefix
		store.Message.Reset(cc.contract)
	}
	if cc.opts.KeepAlive != 0 {
		cc.updateLastAction()
		cc.updateLastTouched()
		go cc.keepalive(ctx)
	}
	go cc.readLoop()      // process incoming messages
	go cc.writeLoop(ctx)  // send messages to servers
	go cc.dispatcher(ctx) // dispatch messages to client

	return nil
}

func (cc *clientConn) attemptConnection(ctx context.Context) error {
	for _, url := range cc.opts.Servers {
		conn, err := grpc.Dial(
			url.Host,
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithTimeout(cc.opts.ConnectTimeout),
		)
		if err != nil {
			return err
		}

		// Connect to grpc stream
		stream, err := pbx.NewUnitdClient(conn).Stream(ctx)
		if err != nil {
			log.Fatal(err)
		}
		cc.conn = StreamConn(stream)

		// get Connect message from options.
		cm := newConnectMsgFromOptions(cc.opts, url)
		rc, connId, _ := Connect(cc.conn, cm)
		if rc == packets.Accepted {
			cc.connID = connId
			cc.messageIds.reset(MID(cc.connID))
			break // successfully connected
		}
		if cc.conn != nil {
			cc.Disconnect()
		}
	}
	return nil
}

// Disconnect will disconnect the connection to the server
func (cc *clientConn) Disconnect() error {
	if err := cc.ok(); err != nil {
		// Disconnect() called but not connected
		return nil
	}
	defer cc.close()
	p := &packets.Disconnect{}
	r := &DisconnectResult{result: result{complete: make(chan struct{})}}
	cc.send <- &PacketAndResult{p: p, r: r}
	return nil
}

// internalConnLost cleanup when connection is lost or an error occurs
func (cc *clientConn) internalConnLost(err error) {
	// It is possible that internalConnLost will be called multiple times simultaneously
	// (including after sending a DisconnectPacket) as such we only do cleanup etc if the
	// routines were actually running and are not being disconnected at users request
	defer cc.close()
	if err := cc.ok(); err == nil {
		if cc.opts.ConnectionLostHandler != nil {
			go cc.opts.ConnectionLostHandler(cc, err)
		}
	}

	fmt.Println("internalConnLost exiting")
}

// Publish will publish a message with the specified QoS and content
// to the specified topic.
func (cc *clientConn) Publish(topic, payload []byte, pubOpts ...PubOptions) Result {
	r := &PublishResult{result: result{complete: make(chan struct{})}}
	if err := cc.ok(); err != nil {
		r.setError(errors.New("error not connected"))
		return r
	}

	opts := new(pubOptions)
	for _, opt := range pubOpts {
		opt.set(opts)
	}

	fh := packets.FixedHeader{Qos: opts.Qos, Retain: opts.Retained}
	pub := &packets.Publish{}
	pub.Topic = topic
	pub.Payload = payload

	if fh.Qos != 0 && pub.MessageID == 0 {
		mID := cc.nextID(r)
		pub.MessageID = cc.outboundID(mID)
	}
	publishWaitTimeout := cc.opts.WriteTimeout
	if publishWaitTimeout == 0 {
		publishWaitTimeout = cc.opts.WriteTimeout
	}
	select {
	case cc.send <- &PacketAndResult{p: pub, r: r}:
	case <-time.After(publishWaitTimeout):
		r.setError(errors.New("publish timeout error occurred"))
		return r
	}
	return r
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (cc *clientConn) Subscribe(topic []byte, subOpts ...SubOptions) Result {
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	if err := cc.ok(); err != nil {
		r.setError(errors.New("error not connected"))
		return r
	}
	opts := new(subOptions)
	for _, opt := range subOpts {
		opt.set(opts)
	}

	sub := &packets.Subscribe{}
	sub.Subscribers = append(sub.Subscribers, &pbx.Subscriber{Topic: topic, Qos: opts.Qos})

	if opts.Callback != nil {
	}

	if sub.MessageID == 0 {
		mID := cc.nextID(r)
		sub.MessageID = cc.outboundID(mID)
	}
	subscribeWaitTimeout := cc.opts.WriteTimeout
	if subscribeWaitTimeout == 0 {
		subscribeWaitTimeout = time.Second * 30
	}
	select {
	case cc.send <- &PacketAndResult{p: sub, r: r}:
	case <-time.After(subscribeWaitTimeout):
		r.setError(errors.New("subscribe timeout error occurred"))
		return r
	}

	return r
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (cc *clientConn) Unsubscribe(topics ...[]byte) Result {
	r := &SubscribeResult{result: result{complete: make(chan struct{})}}
	unsub := &packets.Unsubscribe{}
	var subs []*pbx.Subscriber
	for _, topic := range topics {
		sub := &pbx.Subscriber{Topic: make([]byte, len(topic))}
		copy(sub.Topic, topic)
		subs = append(subs, sub)
	}
	unsub.Subscribers = subs
	if unsub.MessageID == 0 {
		mID := cc.nextID(r)
		unsub.MessageID = cc.outboundID(mID)
	}
	unsubscribeWaitTimeout := cc.opts.WriteTimeout
	if unsubscribeWaitTimeout == 0 {
		unsubscribeWaitTimeout = time.Second * 30
	}
	select {
	case cc.send <- &PacketAndResult{p: unsub, r: r}:
	case <-time.After(unsubscribeWaitTimeout):
		r.setError(errors.New("unsubscribe timeout error occurred"))
		return r
	}
	return r
}

// Load all stored messages and resend them to ensure QOS > 1,2 even after an application crash.
func (cc *clientConn) resume(subscription bool) {
	// contract is used as blockId and key prefix
	keys := store.Message.Keys(cc.contract)
	for _, k := range keys {
		msg := store.Message.Get(k)
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
					//cc.claimID(token, details.MessageID)
					cc.send <- &PacketAndResult{p: msg, r: r}
				}
			case *packets.Unsubscribe:
				if subscription {
					r := &UnsubscribeResult{result: result{complete: make(chan struct{})}}
					cc.send <- &PacketAndResult{p: msg, r: r}
				}

			case *packets.Pubrel:
				cc.send <- &PacketAndResult{p: msg, r: nil}
			case *packets.Publish:
				r := &PublishResult{result: result{complete: make(chan struct{})}}
				r.messageID = info.MessageID
				// cc.claimID(token, details.MessageID)
				cc.send <- &PacketAndResult{p: msg, r: r}
			default:
				store.Message.Delete(k)
			}
		} else {
			switch msg.(type) {
			case *packets.Pubrel:
				cc.recv <- msg
			default:
				store.Message.Delete(k)
			}
		}
	}
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}

func (cc *clientConn) inboundID(id uint32) MID {
	return MID(cc.connID - ((id << 4) | uint32(1)))
}

func (cc *clientConn) outboundID(mid MID) (id uint32) {
	return cc.connID - ((uint32(mid) << 4) | uint32(0))
}

func (cc *clientConn) updateLastAction() {
	if cc.opts.KeepAlive != 0 {
		cc.lastAction.Store(TimeNow())
	}
}

func (cc *clientConn) updateLastTouched() {
	cc.lastTouched.Store(TimeNow())
}

func (cc *clientConn) storeInbound(m packets.Packet) {
	k := uint64(cc.contract)<<32 + uint64(cc.inboundID(m.Info().MessageID))
	store.Message.PersistInbound(k, m)
}

func (cc *clientConn) storeOutbound(m packets.Packet) {
	k := uint64(cc.contract)<<32 + uint64(m.Info().MessageID)
	store.Message.PersistOutbound(k, m)
}

// Set closed flag; return true if not already closed.
func (cc *clientConn) setClosed() bool {
	return atomic.CompareAndSwapUint32(&cc.closed, 0, 1)
}

// Check whether connection was closed.
func (cc *clientConn) isClosed() bool {
	return atomic.LoadUint32(&cc.closed) != 0
}

// Check read ok status.
func (cc *clientConn) ok() error {
	if cc.isClosed() {
		return errors.New("client connection is closed.")
	}
	return nil
}
