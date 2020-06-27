package unitd

import (
	"net/url"
	"sync"

	"github.com/unit-io/unitd-go/packets"
)

// Message defines the externals that a message implementation must support
// these are received messages that are passed, not internal
// messages
type Message interface {
	Topic() []byte
	MessageID() uint32
	Payload() []byte
	Ack()
}

type message struct {
	duplicate bool
	qos       byte
	retained  bool
	topic     []byte
	messageID uint32
	payload   []byte
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

func (m *message) Topic() []byte {
	return m.topic
}

func (m *message) MessageID() uint32 {
	return m.messageID
}

func (m *message) Payload() []byte {
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

	m.CleanSessFlag = opts.CleanSession
	m.ClientID = []byte(opts.ClientID)
	m.InsecureFlag = opts.InsecureFlag

	username := opts.Username
	password := opts.Password
	if server.User != nil {
		username = server.User.Username()
		if pwd, ok := server.User.Password(); ok {
			password = pwd
		}
	}

	if username != "" {
		m.UsernameFlag = true
		m.Username = []byte(username)
		//mustn't have password without user as well
		if password != "" {
			m.PasswordFlag = true
			m.Password = []byte(password)
		}
	}

	m.KeepAlive = uint32(opts.KeepAlive)

	return m
}
