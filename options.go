package unitdb

import (
	"crypto/tls"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	defaultBatchDuration = 100 * time.Millisecond
	maxBatchDuration     = 15 * time.Second
	// publish request (containing a batch of messages) in bytes. Must be lower
	// than the gRPC limit of 4 MiB.
	maxPubBytes = 3.5 * 1024 * 1024
	maxPubCount = 1000
)

// ConnectionHandler is a callback that is called when connection to the server is established.
type ConnectionHandler func(Client)

// ConnectionLostHandler is a callback that is set to be executed
// upon an uninteded disconnection from server.
type ConnectionLostHandler func(Client, error)

type options struct {
	servers                 []*url.URL
	clientID                string
	sessionKey              uint32
	insecureFlag            bool
	username                string
	password                []byte
	cleanSession            bool
	tLSConfig               *tls.Config
	keepAlive               int32
	pingTimeout             time.Duration
	connectTimeout          time.Duration
	storePath               string
	storeSize               int
	storeLogReleaseDuration time.Duration
	connectionHandler       ConnectionHandler
	connectionLostHandler   ConnectionLostHandler
	writeTimeout            time.Duration
	batchDuration           time.Duration
	batchByteThreshold      int
	batchCountThreshold     int
	resumeSubs              bool
}

func (o *options) addServer(target string) {
	re := regexp.MustCompile(`%(25)?`)
	if len(target) > 0 && target[0] == ':' {
		target = "127.0.0.1" + target
	}
	if !strings.Contains(target, "://") {
		target = "grpc://" + target
	}
	target = re.ReplaceAllLiteralString(target, "%25")
	uri, err := url.Parse(target)
	if err != nil {
		return
	}
	o.servers = append(o.servers, uri)
}

func (o *options) setClientID(clientID string) {
	o.clientID = clientID
}

// Options it contains configurable options for client
type Options interface {
	set(*options)
}

// fOption wraps a function that modifies options into an
// implementation of the Option interface.
type fOption struct {
	f func(*options)
}

func (fo *fOption) set(o *options) {
	fo.f(o)
}

func newFuncOption(f func(*options)) *fOption {
	return &fOption{
		f: f,
	}
}

// WithDefaultOptions will create client connection with some default values.
//   CleanSession: True
//   KeepAlive: 30 (seconds)
//   ConnectTimeout: 30 (seconds)
func WithDefaultOptions() Options {
	return newFuncOption(func(o *options) {
		o.servers = nil
		o.clientID = ""
		o.sessionKey = 0
		o.insecureFlag = false
		o.username = ""
		o.password = nil
		o.cleanSession = false
		o.keepAlive = 30
		o.pingTimeout = 30 * time.Second
		o.connectTimeout = 30 * time.Second
		o.writeTimeout = 30 * time.Second // 0 represents timeout disabled
		o.storePath = "/tmp/unitdb"
		o.storeSize = 1 << 27
		if o.writeTimeout > 0 {
			o.storeLogReleaseDuration = o.writeTimeout
		} else {
			o.storeLogReleaseDuration = 1 * time.Minute // must be greater than WriteTimeout
		}
		o.batchDuration = defaultBatchDuration
		o.batchByteThreshold = maxPubBytes
		o.batchCountThreshold = maxPubCount
		o.resumeSubs = false
	})
}

// AddServer returns an Option which makes client connection and set server url
func AddServer(target string) Options {
	return newFuncOption(func(o *options) {
		re := regexp.MustCompile(`%(25)?`)
		if len(target) > 0 && target[0] == ':' {
			target = "127.0.0.1" + target
		}
		if !strings.Contains(target, "://") {
			target = "tcp://" + target
		}
		target = re.ReplaceAllLiteralString(target, "%25")
		uri, err := url.Parse(target)
		if err != nil {
			return
		}
		o.servers = append(o.servers, uri)
	})
}

// WithClientID  returns an Option which makes client connection and set ClientID
func WithClientID(clientID string) Options {
	return newFuncOption(func(o *options) {
		o.clientID = clientID
	})
}

// WithSessionKey  returns an Option which makes client connection with an existing SessionKey
func WithSessionKey(sessKey uint32) Options {
	return newFuncOption(func(o *options) {
		o.sessionKey = sessKey
	})
}

// WithInsecure returns an Option which makes client connection
// with insecure flag so that client can provide topic with key prefix.
// Use insecure flag only for test and debug connection and not for live client.
func WithInsecure() Options {
	return newFuncOption(func(o *options) {
		o.insecureFlag = true
	})
}

// WithUserName returns an Option which makes client connection and pass UserName
func WithUserNamePassword(userName string, password []byte) Options {
	return newFuncOption(func(o *options) {
		o.username = userName
		o.password = password
	})
}

// WithCleanSession returns an Option which makes client connection and set CleanSession
func WithCleanSession() Options {
	return newFuncOption(func(o *options) {
		o.cleanSession = true
	})
}

// WithTLSConfig will set an SSL/TLS configuration to be used when connecting
// to server.
func WithTLSConfig(t *tls.Config) Options {
	return newFuncOption(func(o *options) {
		o.tLSConfig = t
	})
}

// WithKeepAlive will set the amount of time (in seconds) that the client
// should wait before sending a PING request to the server. This will
// allow the client to know that a connection has not been lost with the
// server.
func WithKeepAlive(k time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.keepAlive = int32(k / time.Second)
	})
}

// WithPingTimeout will set the amount of time (in seconds) that the client
// will wait after sending a PING request to the server, before deciding
// that the connection has been lost. Default is 10 seconds.
func WithPingTimeout(k time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.pingTimeout = k
	})
}

// WithWriteTimeout puts a limit on how long a publish should block until it unblocks with a
// timeout error. A duration of 0 never times out. Default never times out
func WithWriteTimeout(t time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.writeTimeout = t
	})
}

// WithConnectTimeout limits how long the client will wait when trying to open a connection
// to server before timing out and erroring the attempt. A duration of 0 never times out.
// Default 30 seconds.
func WithConnectTimeout(t time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.connectTimeout = t
	})
}

// WithStoreDir sets database directory.
func WithStorePath(path string) Options {
	return newFuncOption(func(o *options) {
		o.storePath = path
	})
}

// WithStoreSize sets buffer size store will use to write messages into log.
func WithStoreSize(size int) Options {
	return newFuncOption(func(o *options) {
		o.storeSize = size
	})
}

// WithStoreLogReleaseDuration sets log release duration, it must be greater than WriteTimeout.
func WithStoreLogReleaseDuration(dur time.Duration) Options {
	return newFuncOption(func(o *options) {
		if dur > o.writeTimeout {
			o.storeLogReleaseDuration = dur
		}
	})
}

// WithConnectionHandler sets handler function to be called when client is connected.
func WithConnectionHandler(handler ConnectionHandler) Options {
	return newFuncOption(func(o *options) {
		o.connectionHandler = handler
	})
}

// WithConnectionLostHandler sets handler function to be called
// when connection to the client is lost.
func WithConnectionLostHandler(handler ConnectionLostHandler) Options {
	return newFuncOption(func(o *options) {
		o.connectionLostHandler = handler
	})
}

// WithBatchDuration sets batch duration to group publish requestes into single group.
func WithBatchDuration(dur time.Duration) Options {
	return newFuncOption(func(o *options) {
		if dur < maxBatchDuration {
			o.batchDuration = dur
		} else {
			o.batchDuration = maxBatchDuration
		}
	})
}

// WithBatchByteThreshold sets byte threshold for publish batch.
func WithBatchByteThreshold(size int) Options {
	return newFuncOption(func(o *options) {
		if size < maxPubBytes {
			o.batchByteThreshold = size
		}
	})
}

// WithBatchCountThreshold sets message count threshold for publish batch.
func WithBatchCountThreshold(count int) Options {
	return newFuncOption(func(o *options) {
		if count < maxPubCount {
			o.batchCountThreshold = count
		}
	})
}

// WithResumeSubs will enable resuming stored subscribe/unsubscribe messages
// when connecting but not reconnecting if CleanSession is false.
func WithResumeSubs() Options {
	return newFuncOption(func(o *options) {
		o.resumeSubs = true
	})
}

// -------------------------------------------------------------
type pubSubOptions struct {
	deliveryMode uint8
	delay        int32
}

// -------------------------------------------------------------
type pubOptions struct {
	pubSubOptions
	ttl string
}

// PubOptions it contains configurable options for Publish
type PubOptions interface {
	set(*pubOptions)
}

// fPubOption wraps a function that modifies options into an
// implementation of the PubOption interface.
type fPubOption struct {
	f func(*pubOptions)
}

func (fo *fPubOption) set(o *pubOptions) {
	fo.f(o)
}

func newFuncPubOption(f func(*pubOptions)) *fPubOption {
	return &fPubOption{
		f: f,
	}
}

// WithPubDeliveryMode sets DeliveryMode of publish packet.
// 0 EXPRESS
// 1 RELIEABLE
// 2 BATCH
func WithPubDeliveryMode(deliveryMode uint8) PubOptions {
	return newFuncPubOption(func(o *pubOptions) {
		o.deliveryMode = deliveryMode
	})
}

// WithPubDelay allows to specify delay in milliseconds for delivery of messages
// if DeliveryMode is set to RELIABLE or BATCH.
func WithPubDelay(delay time.Duration) PubOptions {
	return newFuncPubOption(func(o *pubOptions) {
		o.delay = int32(delay.Milliseconds())
	})
}

// WithTTL allows to specify time to live for a publish packet.
func WithTTL(ttl string) PubOptions {
	return newFuncPubOption(func(o *pubOptions) {
		o.ttl = ttl
	})
}

// -------------------------------------------------------------
type relOptions struct {
	last string
}

// Re;Options it contains configurable options for Subscribe
type RelOptions interface {
	set(*relOptions)
}

// fRelOption wraps a function that modifies options into an
// implementation of the RelOption interface.
type fRelOption struct {
	f func(*relOptions)
}

func (fo *fRelOption) set(o *relOptions) {
	fo.f(o)
}

func newFuncRelOption(f func(*relOptions)) *fRelOption {
	return &fRelOption{
		f: f,
	}
}

// WithLast allows to specify duration to retrive stored messages on a new relay request.
func WithLast(last string) RelOptions {
	return newFuncRelOption(func(o *relOptions) {
		o.last = last
	})
}

// -------------------------------------------------------------
type subOptions struct {
	pubSubOptions
}

// SubOptions it contains configurable options for Subscribe
type SubOptions interface {
	set(*subOptions)
}

// fSubOption wraps a function that modifies options into an
// implementation of the SubOption interface.
type fSubOption struct {
	f func(*subOptions)
}

func (fo *fSubOption) set(o *subOptions) {
	fo.f(o)
}

func newFuncSubOption(f func(*subOptions)) *fSubOption {
	return &fSubOption{
		f: f,
	}
}

// WithSubDeliveryMode sets DeliveryMode of a subscription.
// 0 EXPRESS
// 1 RELIEABLE
// 2 BATCH
func WithSubDeliveryMode(deliveryMode uint8) SubOptions {
	return newFuncSubOption(func(o *subOptions) {
		o.deliveryMode = deliveryMode
	})
}

// WithSubDelay allows to specify delay in milliseconds for delivery of messages
// if DeliveryMode is set to RELIABLE or BATCH.
func WithSubDelay(delay time.Duration) SubOptions {
	return newFuncSubOption(func(o *subOptions) {
		o.delay = int32(delay.Milliseconds())
	})
}
