package packets

import (
	"io"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitd/proto"
)

type (
	Subscriber  pbx.Subscriber
	Subscribe   pbx.Subscribe
	Suback      pbx.Suback
	Unsubscribe pbx.Unsubscribe
	Unsuback    pbx.Unsuback
)

// WriteTo writes the encoded Packet to the underlying writer.
func (s *Subscribe) WriteTo(w io.Writer) (int64, error) {
	sub := pbx.Subscribe(*s)
	pkt, err := proto.Marshal(&sub)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (s *Subscribe) Type() pbx.MessageType {
	return pbx.MessageType_SUBSCRIBE
}

// String returns the name of operation.
func (s *Subscribe) String() string {
	return "sub"
}

// WriteTo writes the encoded Packet to the underlying writer.
func (s *Suback) WriteTo(w io.Writer) (int64, error) {
	suback := pbx.Suback(*s)
	pkt, err := proto.Marshal(&suback)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBACK, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (s *Suback) Type() pbx.MessageType {
	return pbx.MessageType_SUBACK
}

// String returns the name of operation.
func (s *Suback) String() string {
	return "suback"
}

// Write writes the encoded Packet to the underlying writer.
func (u *Unsubscribe) WriteTo(w io.Writer) (int64, error) {
	unsub := pbx.Unsubscribe(*u)
	pkt, err := proto.Marshal(&unsub)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (u *Unsubscribe) Type() pbx.MessageType {
	return pbx.MessageType_UNSUBSCRIBE
}

// String returns the name of operation.
func (u *Unsubscribe) String() string {
	return "unsub"
}

// WriteTo writes the encoded Packet to the underlying writer.
func (u *Unsuback) WriteTo(w io.Writer) (int64, error) {
	unusuback := pbx.Unsuback(*u)
	pkt, err := proto.Marshal(&unusuback)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBACK, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (u *Unsuback) Type() pbx.MessageType {
	return pbx.MessageType_UNSUBACK
}

// String returns the name of operation.
func (u *Unsuback) String() string {
	return "unsuback"
}

func unpackSubscribe(data []byte) Packet {
	var pkt pbx.Subscribe
	proto.Unmarshal(data, &pkt)
	return &Subscribe{
		MessageID:   pkt.MessageID,
		Subscribers: pkt.Subscribers,
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
	return &Unsubscribe{
		MessageID:   pkt.MessageID,
		Subscribers: pkt.Subscribers,
	}
}

func unpackUnsuback(data []byte) Packet {
	var pkt pbx.Unsuback
	proto.Unmarshal(data, &pkt)
	return &Unsuback{
		MessageID: pkt.MessageID,
	}
}
