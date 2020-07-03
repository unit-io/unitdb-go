package unitd

import (
	"crypto/tls"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// MessageHandler is a callback type which can be set to be
// executed upon the arrival of messages published to topics
// to which the client is subscribed.
type MessageHandler func(ClientConn, Message)

// ConnectionHandler is a callback that is called when connection to the server is established.
type ConnectionHandler func(ClientConn)

// ConnectionLostHandler is a callback that is set to be executed
// upon an uninteded disconnection from server.
type ConnectionLostHandler func(ClientConn, error)

type options struct {
	Servers                 []*url.URL
	ClientID                string
	InsecureFlag            bool
	Username                string
	Password                string
	CleanSession            bool
	TLSConfig               *tls.Config
	KeepAlive               int64
	PingTimeout             time.Duration
	ConnectTimeout          time.Duration
	StorePath               string
	StoreSize               int
	StoreLogReleaseDuration time.Duration
	DefaultMessageHandler   MessageHandler
	ConnectionHandler       ConnectionHandler
	ConnectionLostHandler   ConnectionLostHandler
	WriteTimeout            time.Duration
	ResumeSubs              bool
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
	o.Servers = append(o.Servers, uri)
}

func (o *options) setClientID(clientID string) {
	o.ClientID = clientID
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
		o.Servers = nil
		o.ClientID = ""
		o.InsecureFlag = false
		o.Username = ""
		o.Password = ""
		o.CleanSession = true
		o.KeepAlive = 60
		o.PingTimeout = 60 * time.Second
		o.ConnectTimeout = 60 * time.Second
		o.WriteTimeout = 60 * time.Second // 0 represents timeout disabled
		o.StorePath = "/tmp/unitdb"
		o.StoreSize = 1 << 27
		if o.WriteTimeout > 0 {
			o.StoreLogReleaseDuration = o.WriteTimeout
		} else {
			o.StoreLogReleaseDuration = 1 * time.Minute // must be greater than WriteTimeout
		}
		o.ResumeSubs = false
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
		o.Servers = append(o.Servers, uri)
	})
}

// WithClientID  returns an Option which makes client connection and set ClientID
func WithClientID(clientID string) Options {
	return newFuncOption(func(o *options) {
		o.ClientID = clientID
	})
}

// WithInsecure returns an Option which makes client connection
// with insecure flag so that client can provide topic with key prefix.
// Use insecure flag only for test and debug connection and not for live client.
func WithInsecure() Options {
	return newFuncOption(func(o *options) {
		o.InsecureFlag = true
	})
}

// WithUserName returns an Option which makes client connection and pass UserName
func WithUserNamePassword(userName, password string) Options {
	return newFuncOption(func(o *options) {
		o.Username = userName
		o.Password = password
	})
}

// WithCleanSession returns an Option which makes client connection and set CleanSession
func WithCleanSession() Options {
	return newFuncOption(func(o *options) {
		o.CleanSession = true
	})
}

// WithTLSConfig will set an SSL/TLS configuration to be used when connecting
// to server.
func WithTLSConfig(t *tls.Config) Options {
	return newFuncOption(func(o *options) {
		o.TLSConfig = t
	})
}

// WithKeepAlive will set the amount of time (in seconds) that the client
// should wait before sending a PING request to the server. This will
// allow the client to know that a connection has not been lost with the
// server.
func WithKeepAlive(k time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.KeepAlive = int64(k / time.Second)
	})
}

// WithPingTimeout will set the amount of time (in seconds) that the client
// will wait after sending a PING request to the server, before deciding
// that the connection has been lost. Default is 10 seconds.
func WithPingTimeout(k time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.PingTimeout = k
	})
}

// WithWriteTimeout puts a limit on how long a publish should block until it unblocks with a
// timeout error. A duration of 0 never times out. Default never times out
func WithWriteTimeout(t time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.WriteTimeout = t
	})
}

// WithConnectTimeout limits how long the client will wait when trying to open a connection
// to server before timing out and erroring the attempt. A duration of 0 never times out.
// Default 30 seconds.
func WithConnectTimeout(t time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.ConnectTimeout = t
	})
}

// WithStoreDir sets database directory.
func WithStorePath(path string) Options {
	return newFuncOption(func(o *options) {
		o.StorePath = path
	})
}

// WithStoreSize sets buffer size store will use to write messages into log.
func WithStoreSize(size int) Options {
	return newFuncOption(func(o *options) {
		o.StoreSize = size
	})
}

// WithStoreLogReleaseDuration sets log release duration, it must be greater than WriteTimeout.
func WithStoreLogReleaseDuration(dur time.Duration) Options {
	return newFuncOption(func(o *options) {
		if dur > o.WriteTimeout {
			o.StoreLogReleaseDuration = dur
		}
	})
}

// WithDefaultMessageHandler set default message handler to be called
// on message receive to all topics client has subscribed to.
func WithDefaultMessageHandler(defaultHandler MessageHandler) Options {
	return newFuncOption(func(o *options) {
		o.DefaultMessageHandler = defaultHandler
	})
}

// WithConnectionHandler set handler function to be called when client is connected.
func WithConnectionHandler(handler ConnectionHandler) Options {
	return newFuncOption(func(o *options) {
		o.ConnectionHandler = handler
	})
}

// WithConnectionLostHandler set handler function to be called
// when connection to the client is lost.
func WithConnectionLostHandler(handler ConnectionLostHandler) Options {
	return newFuncOption(func(o *options) {
		o.ConnectionLostHandler = handler
	})
}

// WithResumeSubs will enable resuming stored subscribe/unsubscribe messages
// when connecting but not reconnecting if CleanSession is false.
func WithResumeSubs() Options {
	return newFuncOption(func(o *options) {
		o.ResumeSubs = true
	})
}

// -------------------------------------------------------------
type pubOptions struct {
	Qos      uint32
	Retained bool
}

// SubOptions it contains configurable options for Subscribe
type PubOptions interface {
	set(*pubOptions)
}

// fSubOption wraps a function that modifies options into an
// implementation of the SubOption interface.
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

func WithPubQos(qos uint32) PubOptions {
	return newFuncPubOption(func(o *pubOptions) {
		o.Qos = qos
	})
}

func WithRetained() PubOptions {
	return newFuncPubOption(func(o *pubOptions) {
		o.Retained = true
	})
}

// -------------------------------------------------------------
type subOptions struct {
	Qos      uint32
	Callback MessageHandler
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

func WithSubQos(qos uint32) SubOptions {
	return newFuncSubOption(func(o *subOptions) {
		o.Qos = qos
	})
}

// WithCallback sets handler function to be called
// upon receiving published message for topic client has Subscribed to.
func WithCallback(handler MessageHandler) SubOptions {
	return newFuncSubOption(func(o *subOptions) {
		o.Callback = handler
	})
}
