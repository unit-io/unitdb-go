package utp

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type (
	Subscription pbx.Subscription
	Subscribe    struct {
		MessageID     int32
		Subscriptions []*Subscription
	}
	Suback      pbx.Suback
	Unsubscribe struct {
		MessageID     int32
		Subscriptions []*Subscription
	}
	Unsuback pbx.Unsuback
)

func encodeSubscribe(s Subscribe) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscription
	for _, sub := range s.Subscriptions {
		s := pbx.Subscription(*sub)
		subs = append(subs, &s)
	}
	sub := pbx.Subscribe{
		MessageID:     s.MessageID,
		Subscriptions: subs,
	}
	pkt, err := proto.Marshal(&sub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (s *Subscribe) Type() MessageType {
	return MessageType(pbx.MessageType_SUBSCRIBE)
}

// Info returns DeliveryMode and MessageID of this packet.
func (s *Subscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: s.MessageID}
}

func encodeSuback(s Suback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	suback := pbx.Suback(s)
	pkt, err := proto.Marshal(&suback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBACK, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (s *Suback) Type() MessageType {
	return MessageType(pbx.MessageType_SUBACK)
}

// Info returns DeliveryMode and MessageID of this packet.
func (s *Suback) Info() Info {
	return Info{DeliveryMode: 0, MessageID: s.MessageID}
}

func encodeUnsubscribe(u Unsubscribe) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscription
	for _, sub := range u.Subscriptions {
		s := pbx.Subscription(*sub)
		subs = append(subs, &s)
	}
	unsub := pbx.Unsubscribe{
		MessageID:     u.MessageID,
		Subscriptions: subs,
	}
	pkt, err := proto.Marshal(&unsub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (u *Unsubscribe) Type() MessageType {
	return MessageType(pbx.MessageType_UNSUBSCRIBE)
}

// Info returns DeliveryMode and MessageID of this packet.
func (u *Unsubscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: u.MessageID}
}

func encodeUnsuback(u Unsuback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	unusuback := pbx.Unsuback(u)
	pkt, err := proto.Marshal(&unusuback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBACK, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (u *Unsuback) Type() MessageType {
	return MessageType(pbx.MessageType_UNSUBACK)
}

// Info returns DeliveryMode and MessageID of this packet.
func (u *Unsuback) Info() Info {
	return Info{DeliveryMode: 0, MessageID: u.MessageID}
}

func unpackSuback(data []byte) Packet {
	var pkt pbx.Suback
	proto.Unmarshal(data, &pkt)
	return &Suback{
		MessageID: pkt.MessageID,
	}
}

func unpackUnsuback(data []byte) Packet {
	var pkt pbx.Unsuback
	proto.Unmarshal(data, &pkt)
	return &Unsuback{
		MessageID: pkt.MessageID,
	}
}
