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
	Unsubscribe struct {
		MessageID     int32
		Subscriptions []*Subscription
	}
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
	rawMsg, err := proto.Marshal(&sub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (s *Subscribe) Type() MessageType {
	return SUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (s *Subscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: s.MessageID}
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
	rawMsg, err := proto.Marshal(&unsub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (u *Unsubscribe) Type() MessageType {
	return UNSUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (u *Unsubscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: u.MessageID}
}
