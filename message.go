package unitdb

import (
	"net/url"
	"sync"

	"github.com/unit-io/unitdb-go/utp"
)

// Message defines the externals that a message implementation must support
// these are received messages that are passed, not internal
// messages
type Message interface {
	Topic() []byte
	MessageID() int32
	Payload() []byte
	Ack()
}

type message struct {
	duplicate    bool
	deliveryMode byte
	retained     bool
	topic        []byte
	messageID    int32
	payload      []byte
	once         sync.Once
	ack          func()
}

func (m *message) Duplicate() bool {
	return m.duplicate
}

func (m *message) DeliveryMode() byte {
	return m.deliveryMode
}

func (m *message) Retained() bool {
	return m.retained
}

func (m *message) Topic() []byte {
	return m.topic
}

func (m *message) MessageID() int32 {
	return m.messageID
}

func (m *message) Payload() []byte {
	return m.payload
}

func (m *message) Ack() {
	m.once.Do(m.ack)
}

func messageFromPublish(p *utp.Publish, ack func()) Message {
	return &message{
		topic:     p.Topic,
		messageID: p.MessageID,
		payload:   p.Payload,
		ack:       ack,
	}
}

func newConnectMsgFromOptions(opts *options, server *url.URL) *utp.Connect {
	m := &utp.Connect{}

	m.CleanSessFlag = opts.cleanSession
	m.ClientID = opts.clientID
	m.InsecureFlag = opts.insecureFlag

	username := opts.username
	password := opts.password
	if server.User != nil {
		username = []byte(server.User.Username())
		if pwd, ok := server.User.Password(); ok {
			password = []byte(pwd)
		}
	}

	if username != nil {
		m.Username = username
		//mustn't have password without user as well
		if password != nil {
			m.Password = password
		}
	}

	m.KeepAlive = int32(opts.keepAlive)

	return m
}
