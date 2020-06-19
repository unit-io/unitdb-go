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
	plugins "github.com/unit-io/unitd/plugins/grpc"
	pbx "github.com/unit-io/unitd/proto"
	"google.golang.org/grpc"
)

type ClientConn interface {
	// Connect will create a connection to the server
	Connect() error
	// Disconnect will end the connection with the server, but not before waiting
	// the client wait group is done.
	Disconnect() error
	// Publish will publish a message with the specified QoS and content
	// to the specified topic.
	Publish(topic, payload []byte) error
	// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
	// a message is published on the topic provided, or nil for the default handler
	Subscribe(topic []byte, subOpts ...SubOptions) error
	// Unsubscribe will end the subscription from each of the topics provided.
	// Messages published to those topics from other clients will no longer be
	// received.
	Unsubscribe(topics ...[]byte) error
	// Context return when GRPC stream context
	Context() context.Context
}
type clientConn struct {
	mu        sync.Mutex // mutex for the connection
	opts      *options
	context   context.Context
	cancel    context.CancelFunc // cancellation function
	conn      net.Conn           // the network connection
	stream    grpc.Stream
	send      chan []byte
	recv      chan *packets.Publish
	callbacks map[uint64]MessageHandler

	// Time when the keepalive session was last refreshed
	lastTouched atomic.Value
	// Time when the session received any packer from client
	lastAction atomic.Value

	// Close.
	closeW sync.WaitGroup
	closeC chan struct{}
	closed uint32
}

func NewClient(target string, clientID string, opts ...Options) (ClientConn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cc := &clientConn{
		opts:      new(options),
		context:   ctx,
		cancel:    cancel,
		send:      make(chan []byte, 1), // buffered
		recv:      make(chan *packets.Publish),
		callbacks: make(map[uint64]MessageHandler),
		// Close
		closeC: make(chan struct{}),
	}
	WithDefaultOptions().set(cc.opts)
	for _, opt := range opts {
		opt.set(cc.opts)
	}
	// set default options
	cc.opts.addServer(target)
	cc.opts.setClientID(clientID)
	cc.callbacks[0] = cc.opts.DefaultMessageHandler
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

	// Signal all goroutines.
	close(cc.closeC)

	// Wait for all goroutines to exit.
	cc.closeW.Wait()
	close(cc.send)
	close(cc.recv)
	if cc.cancel != nil {
		cc.cancel()
	}
	return nil
}

// Connect will create a connection to the server
func (cc *clientConn) Connect() error {
	// if err := cc.ok(); err == nil {
	// 	// Connect() called but not disconnected
	// 	return nil
	// }

	// Connect to the server
	if len(cc.opts.Servers) == 0 {
		return errors.New("no servers defined to connect to")
	}
	if err := cc.attemptConnection(); err != nil {
		return err
	}

	if cc.opts.KeepAlive != 0 {
		cc.updateLastAction()
		cc.updateLastTouched()
		go cc.keepalive()
	}
	go cc.readLoop()   // process incoming messages
	go cc.writeLoop()  // send messages to servers
	go cc.dispatcher() // dispatch messages to client

	return nil
}

func (cc *clientConn) attemptConnection() error {
	for _, url := range cc.opts.Servers {
		conn, err := grpc.Dial(
			url.Host,
			grpc.WithBlock(),
			grpc.WithInsecure())
		if err != nil {
			return err
		}

		// Connect for streaming
		stream, err := pbx.NewUnitdClient(conn).Stream(cc.context)
		if err != nil {
			log.Fatal(err)
		}
		cc.conn = StreamConn(stream)

		// get Connect message from options.
		cm := newConnectMsgFromOptions(cc.opts, url)
		rc, _ := Connect(cc.conn, cm)
		if rc == packets.Accepted {
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
	defer cc.close()
	if err := cc.ok(); err != nil {
		// Disconnect() called but not connected
		return nil
	}

	dm := packets.Disconnect{}
	_, err := dm.WriteTo(cc.conn)
	return err
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
func (cc *clientConn) Publish(topic, payload []byte) error {
	if err := cc.ok(); err != nil {
		return errors.New("error not connected")
	}
	pub := packets.Publish{}
	pub.Topic = topic
	pub.Payload = payload

	publishWaitTimeout := cc.opts.WriteTimeout
	if publishWaitTimeout == 0 {
		publishWaitTimeout = time.Second * 30
	}
	if _, err := pub.WriteTo(cc.conn); err != nil {
		return err
	}
	return nil
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (cc *clientConn) Subscribe(topic []byte, subOpts ...SubOptions) error {
	if err := cc.ok(); err != nil {
		return errors.New("error not connected")
	}
	opts := new(subOptions)
	for _, opt := range subOpts {
		opt.set(opts)
	}

	sub := packets.Subscribe{}
	sub.Subscribers = append(sub.Subscribers, &pbx.Subscriber{Topic: topic, Qos: opts.Qos})

	if opts.Callback != nil {
	}

	if sub.MessageID == 0 {
		sub.MessageID = 0 // TODO
	}
	subscribeWaitTimeout := cc.opts.WriteTimeout
	if subscribeWaitTimeout == 0 {
		subscribeWaitTimeout = time.Second * 30
	}
	if _, err := sub.WriteTo(cc.conn); err != nil {
		return err
	}

	return nil
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (cc *clientConn) Unsubscribe(topics ...[]byte) error {
	unsub := packets.Unsubscribe{}
	var subs []*pbx.Subscriber
	for _, topic := range topics {
		sub := &pbx.Subscriber{Topic: make([]byte, len(topic))}
		copy(sub.Topic, topic)
		subs = append(subs, sub)
	}
	unsub.Subscribers = subs
	unsubscribeWaitTimeout := cc.opts.WriteTimeout
	if unsubscribeWaitTimeout == 0 {
		unsubscribeWaitTimeout = time.Second * 30
	}
	unsub.WriteTo(cc.conn)
	return nil
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}

func (cc *clientConn) updateLastAction() {
	if cc.opts.KeepAlive != 0 {
		cc.lastAction.Store(TimeNow())
	}
}

func (cc *clientConn) updateLastTouched() {
	cc.lastTouched.Store(TimeNow())
}

// Context return when GRPC stream context
func (cc *clientConn) Context() context.Context {
	return cc.context
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
