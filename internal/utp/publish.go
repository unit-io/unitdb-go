package utp

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type (
	PublishMessage pbx.PublishMessage
	Publish        struct {
		MessageID    int32
		DeliveryMode int32
		Messages     []*PublishMessage
	}
)

func encodePublish(p Publish) (bytes.Buffer, error) {
	var msg bytes.Buffer

	var msgs []*pbx.PublishMessage
	for _, m := range p.Messages {
		pubMsg := pbx.PublishMessage(*m)
		msgs = append(msgs, &pubMsg)
	}
	pub := pbx.Publish{
		MessageID:    p.MessageID,
		DeliveryMode: p.DeliveryMode,
		Messages:     msgs,
	}
	rawMsg, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (p *Publish) Type() MessageType {
	return PUBLISH
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Publish) Info() Info {
	return Info{DeliveryMode: p.DeliveryMode, MessageID: p.MessageID}
}

func unpackPublish(data []byte) Message {
	var pub pbx.Publish
	proto.Unmarshal(data, &pub)
	var msgs []*PublishMessage
	for _, m := range pub.Messages {
		pubMsg := &PublishMessage{
			Topic:   m.Topic,
			Payload: m.Payload,
			Ttl:     m.Ttl,
		}
		msgs = append(msgs, pubMsg)
	}
	return &Publish{
		MessageID:    pub.MessageID,
		DeliveryMode: pub.DeliveryMode,
		Messages:     msgs,
	}
}
