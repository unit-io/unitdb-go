package unitdb

import (
	"net/url"
	"sync"

	"github.com/unit-io/unitdb/server/utp"
)

// PubMessage defines the externals that a message implementation must support
// these are received messages that are passed, not internal
// messages
type PubMessage interface {
	DeliveryMode() uint8
	MessageID() uint16
	Messages() []*Message
	Ack()
}

type Message struct {
	Topic   string
	Payload []byte
}

type pubMessage struct {
	deliveryMode uint8
	messageID    uint16
	messages     []*Message
	once         sync.Once
	ack          func()
}

func (m *pubMessage) DeliveryMode() uint8 {
	return m.deliveryMode
}

func (m *pubMessage) MessageID() uint16 {
	return m.messageID
}

func (m *pubMessage) Messages() []*Message {
	return m.messages
}

func (m *pubMessage) Ack() {
	m.once.Do(m.ack)
}

func messageFromPublish(p *utp.Publish, ack func()) *pubMessage {
	var messages []*Message
	for _, pubMsg := range p.Messages {
		msg := &Message{
			Topic:   pubMsg.Topic,
			Payload: pubMsg.Payload,
		}
		messages = append(messages, msg)
	}

	return &pubMessage{
		deliveryMode: p.DeliveryMode,
		messageID:    p.MessageID,
		messages:     messages,
		ack:          ack,
	}
}

func newConnectMsgFromOptions(opts *options, server *url.URL) *utp.Connect {
	m := &utp.Connect{}

	m.CleanSessFlag = opts.cleanSession
	m.ClientID = opts.clientID
	m.SessKey = int32(opts.sessionKey)
	m.InsecureFlag = opts.insecureFlag

	username := opts.username
	password := opts.password
	if server.User != nil {
		username = server.User.Username()
		if pwd, ok := server.User.Password(); ok {
			password = []byte(pwd)
		}
	}

	if username != "" {
		m.Username = username
		//mustn't have password without user as well
		if password != nil {
			m.Password = password
		}
	}

	m.KeepAlive = int32(opts.keepAlive)
	m.BatchDuration = int32(opts.batchDuration.Milliseconds())
	m.BatchByteThreshold = int32(opts.batchByteThreshold)
	m.BatchCountThreshold = int32(opts.batchCountThreshold)

	return m
}
