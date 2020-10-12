package packets

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unite/proto"
)

type (
	Subscriber pbx.Subscriber
	Subscribe  struct {
		MessageID   int32
		Subscribers []*Subscriber
	}
	Suback      pbx.Suback
	Unsubscribe struct {
		MessageID   int32
		Subscribers []*Subscriber
	}
	Unsuback pbx.Unsuback
)

func encodeSubscribe(s Subscribe) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscriber
	for _, sub := range s.Subscribers {
		s := pbx.Subscriber(*sub)
		subs = append(subs, &s)
	}
	sub := pbx.Subscribe{
		MessageID:   s.MessageID,
		Subscribers: subs,
	}
	pkt, err := proto.Marshal(&sub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (s *Subscribe) Type() MessageType {
	return MessageType(pbx.MessageType_SUBSCRIBE)
}

// Info returns Qos and MessageID of this packet.
func (s *Subscribe) Info() Info {
	return Info{Qos: 1, MessageID: s.MessageID}
}

func encodeSuback(s Suback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	suback := pbx.Suback(s)
	pkt, err := proto.Marshal(&suback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (s *Suback) Type() MessageType {
	return MessageType(pbx.MessageType_SUBACK)
}

// Info returns Qos and MessageID of this packet.
func (s *Suback) Info() Info {
	return Info{Qos: 0, MessageID: s.MessageID}
}

func encodeUnsubscribe(u Unsubscribe) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscriber
	for _, sub := range u.Subscribers {
		s := pbx.Subscriber(*sub)
		subs = append(subs, &s)
	}
	unsub := pbx.Unsubscribe{
		MessageID:   u.MessageID,
		Subscribers: subs,
	}
	pkt, err := proto.Marshal(&unsub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (u *Unsubscribe) Type() MessageType {
	return MessageType(pbx.MessageType_UNSUBSCRIBE)
}

// Info returns Qos and MessageID of this packet.
func (u *Unsubscribe) Info() Info {
	return Info{Qos: 1, MessageID: u.MessageID}
}

func encodeUnsuback(u Unsuback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	unusuback := pbx.Unsuback(u)
	pkt, err := proto.Marshal(&unusuback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (u *Unsuback) Type() MessageType {
	return MessageType(pbx.MessageType_UNSUBACK)
}

// Info returns Qos and MessageID of this packet.
func (u *Unsuback) Info() Info {
	return Info{Qos: 0, MessageID: u.MessageID}
}

func unpackSubscribe(data []byte) Packet {
	var pkt pbx.Subscribe
	proto.Unmarshal(data, &pkt)
	var subs []*Subscriber
	for _, sub := range pkt.Subscribers {
		s := &Subscriber{
			Topic: sub.Topic,
			Qos:   sub.Qos,
		}
		subs = append(subs, s)
	}
	return &Subscribe{
		MessageID:   pkt.MessageID,
		Subscribers: subs,
	}
}

func unpackSuback(data []byte) Packet {
	var pkt pbx.Suback
	proto.Unmarshal(data, &pkt)
	return &Suback{
		MessageID: pkt.MessageID,
		Qos:       pkt.Qos,
	}
}

func unpackUnsubscribe(data []byte) Packet {
	var pkt pbx.Unsubscribe
	proto.Unmarshal(data, &pkt)
	var subs []*Subscriber
	for _, sub := range pkt.Subscribers {
		s := &Subscriber{
			Topic: sub.Topic,
		}
		subs = append(subs, s)
	}
	return &Unsubscribe{
		MessageID:   pkt.MessageID,
		Subscribers: subs,
	}
}

func unpackUnsuback(data []byte) Packet {
	var pkt pbx.Unsuback
	proto.Unmarshal(data, &pkt)
	return &Unsuback{
		MessageID: pkt.MessageID,
	}
}
