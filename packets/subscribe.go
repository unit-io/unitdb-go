package packets

import (
	"bytes"
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

func (s *Subscribe) encode() (bytes.Buffer, error) {
	var buf bytes.Buffer
	sub := pbx.Subscribe(*s)
	pkt, err := proto.Marshal(&sub)
	if err != nil {
		return buf, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, RemainingLength: uint32(len(pkt))}
	buf = fh.pack()
	_, err = buf.Write(pkt)
	return buf, err
}

// Encode encodes message into binary data
func (s *Subscribe) Encode() []byte {
	buf, err := s.encode()
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (s *Subscribe) WriteTo(w io.Writer) (int64, error) {
	buf, err := s.encode()
	if err != nil {
		return 0, err
	}
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (s *Subscribe) Type() MessageType {
	return MessageType(pbx.MessageType_SUBSCRIBE)
}

// String returns the name of operation.
func (s *Subscribe) String() string {
	return "sub"
}

// Info returns Qos and MessageID of this packet.
func (s *Subscribe) Info() Info {
	return Info{Qos: 1, MessageID: s.MessageID}
}

func (s *Suback) encode() (bytes.Buffer, error) {
	var buf bytes.Buffer
	suback := pbx.Suback(*s)
	pkt, err := proto.Marshal(&suback)
	if err != nil {
		return buf, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBACK, RemainingLength: uint32(len(pkt))}
	buf = fh.pack()
	_, err = buf.Write(pkt)
	return buf, err
}

// Encode encodes message into binary data
func (s *Suback) Encode() []byte {
	buf, err := s.encode()
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (s *Suback) WriteTo(w io.Writer) (int64, error) {
	buf, err := s.encode()
	if err != nil {
		return 0, err
	}
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (s *Suback) Type() MessageType {
	return MessageType(pbx.MessageType_SUBACK)
}

// String returns the name of operation.
func (s *Suback) String() string {
	return "suback"
}

// Info returns Qos and MessageID of this packet.
func (s *Suback) Info() Info {
	return Info{Qos: 0, MessageID: s.MessageID}
}

func (u *Unsubscribe) encode() (bytes.Buffer, error) {
	var buf bytes.Buffer
	unsub := pbx.Unsubscribe(*u)
	pkt, err := proto.Marshal(&unsub)
	if err != nil {
		return buf, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, RemainingLength: uint32(len(pkt))}
	buf = fh.pack()
	_, err = buf.Write(pkt)
	return buf, err
}

// Encode encodes message into binary data
func (u *Unsubscribe) Encode() []byte {
	buf, err := u.encode()
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// Write writes the encoded Packet to the underlying writer.
func (u *Unsubscribe) WriteTo(w io.Writer) (int64, error) {
	buf, err := u.encode()
	if err != nil {
		return 0, err
	}
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (u *Unsubscribe) Type() MessageType {
	return MessageType(pbx.MessageType_UNSUBSCRIBE)
}

// String returns the name of operation.
func (u *Unsubscribe) String() string {
	return "unsub"
}

// Info returns Qos and MessageID of this packet.
func (u *Unsubscribe) Info() Info {
	return Info{Qos: 1, MessageID: u.MessageID}
}

func (u *Unsuback) encode() (bytes.Buffer, error) {
	var buf bytes.Buffer
	unusuback := pbx.Unsuback(*u)
	pkt, err := proto.Marshal(&unusuback)
	if err != nil {
		return buf, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBACK, RemainingLength: uint32(len(pkt))}
	buf = fh.pack()
	_, err = buf.Write(pkt)
	return buf, err
}

// Encode encodes message into binary data
func (u *Unsuback) Encode() []byte {
	buf, err := u.encode()
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (u *Unsuback) WriteTo(w io.Writer) (int64, error) {
	buf, err := u.encode()
	if err != nil {
		return 0, err
	}
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (u *Unsuback) Type() MessageType {
	return MessageType(pbx.MessageType_UNSUBACK)
}

// String returns the name of operation.
func (u *Unsuback) String() string {
	return "unsuback"
}

// Info returns Qos and MessageID of this packet.
func (u *Unsuback) Info() Info {
	return Info{Qos: 0, MessageID: u.MessageID}
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
