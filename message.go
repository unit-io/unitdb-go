package unitdb

import (
	"net/url"
	"sync"

	"github.com/unit-io/unitdb-go/packets"
)

// Message defines the externals that a message implementation must support
// these are received messages that are passed, not internal
// messages
type Message interface {
	Topic() string
	MessageID() int32
	Payload() string
	Ack()
}

type message struct {
	duplicate bool
	qos       byte
	retained  bool
	topic     string
	messageID int32
	payload   string
	once      sync.Once
	ack       func()
}

func (m *message) Duplicate() bool {
	return m.duplicate
}

func (m *message) Qos() byte {
	return m.qos
}

func (m *message) Retained() bool {
	return m.retained
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) MessageID() int32 {
	return m.messageID
}

func (m *message) Payload() string {
	return m.payload
}

func (m *message) Ack() {
	m.once.Do(m.ack)
}

func messageFromPublish(p *packets.Publish, ack func()) Message {
	return &message{
		topic:     p.Topic,
		messageID: p.MessageID,
		payload:   p.Payload,
		ack:       ack,
	}
}

func newConnectMsgFromOptions(opts *options, server *url.URL) *packets.Connect {
	m := &packets.Connect{}

	m.CleanSessFlag = opts.cleanSession
	m.ClientID = opts.clientID
	m.InsecureFlag = opts.insecureFlag

	username := opts.username
	password := opts.password
	if server.User != nil {
		username = server.User.Username()
		if pwd, ok := server.User.Password(); ok {
			password = pwd
		}
	}

	if username != "" {
		m.UsernameFlag = true
		m.Username = username
		//mustn't have password without user as well
		if password != "" {
			m.PasswordFlag = true
			m.Password = password
		}
	}

	m.KeepAlive = int32(opts.keepAlive)

	return m
}
