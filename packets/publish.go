package packets

import (
	"bytes"
	"io"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitd/proto"
)

type (
	Publish pbx.Publish
	Puback  pbx.Puback
	Pubrec  pbx.Pubrec
	Pubrel  pbx.Pubrel
	Pubcomp pbx.Pubcomp
)

func (p *Publish) encode() (bytes.Buffer, error) {
	var msg bytes.Buffer
	pub := pbx.Publish(*p)
	pkt, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, RemainingLength: uint32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Encode encodes message into binary data
func (p *Publish) Encode() []byte {
	buf, err := p.encode()
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// WriteTo writes the encoded message to the buffer.
func (p *Publish) WriteTo(w io.Writer) (int64, error) {
	msg, err := p.encode()
	if err != nil {
		return 0, err
	}
	return msg.WriteTo(w)
}

// Type returns the packet type.
func (p *Publish) Type() MessageType {
	return MessageType(pbx.MessageType_PUBLISH)
}

// String returns the name of operation.
func (p *Publish) String() string {
	return "pub"
}

// Info returns Qos and MessageID of this packet.
func (p *Publish) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

func (p *Puback) encode() (bytes.Buffer, error) {
	var msg bytes.Buffer
	puback := pbx.Puback(*p)
	pkt, err := proto.Marshal(&puback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBACK, RemainingLength: uint32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Encode encodes message into binary data
func (p *Puback) Encode() []byte {
	msg, err := p.encode()
	if err != nil {
		return nil
	}
	return msg.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Puback) WriteTo(w io.Writer) (int64, error) {
	msg, err := p.encode()
	if err != nil {
		return 0, err
	}
	return msg.WriteTo(w)
}

// Type returns the Packet type.
func (p *Puback) Type() MessageType {
	return MessageType(pbx.MessageType_PUBACK)
}

// String returns the name of operation.
func (p *Puback) String() string {
	return "puback"
}

// Info returns Qos and MessageID of this packet.
func (p *Puback) Info() Info {
	return Info{Qos: 0, MessageID: p.MessageID}
}

func (p *Pubrec) encode() (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrec := pbx.Pubrec(*p)
	pkt, err := proto.Marshal(&pubrec)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREC, RemainingLength: uint32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Encode encodes message into binary data
func (p *Pubrec) Encode() []byte {
	msg, err := p.encode()
	if err != nil {
		return nil
	}
	return msg.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Pubrec) WriteTo(w io.Writer) (int64, error) {
	msg, err := p.encode()
	if err != nil {
		return 0, err
	}
	return msg.WriteTo(w)
}

// Type returns the Packet type.
func (p *Pubrec) Type() MessageType {
	return MessageType(pbx.MessageType_PUBREC)
}

// String returns the name of operation.
func (p *Pubrec) String() string {
	return "pubrec"
}

// Info returns Qos and MessageID of this packet.
func (p *Pubrec) Info() Info {
	return Info{Qos: 0, MessageID: p.MessageID}
}

func (p *Pubrel) encode() (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrel := pbx.Pubrel(*p)
	pkt, err := proto.Marshal(&pubrel)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREL, RemainingLength: uint32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Encode encodes message into binary data
func (p *Pubrel) Encode() []byte {
	msg, err := p.encode()
	if err != nil {
		return nil
	}
	return msg.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Pubrel) WriteTo(w io.Writer) (int64, error) {
	msg, err := p.encode()
	if err != nil {
		return 0, err
	}
	return msg.WriteTo(w)
}

// Type returns the Packet type.
func (p *Pubrel) Type() MessageType {
	return MessageType(pbx.MessageType_PUBREL)
}

// String returns the name of operation.
func (p *Pubrel) String() string {
	return "pubrel"
}

// Info returns Qos and MessageID of this packet.
func (p *Pubrel) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

func (p *Pubcomp) encode() (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubcomp := pbx.Pubcomp(*p)
	pkt, err := proto.Marshal(&pubcomp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBCOMP, RemainingLength: uint32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Encode encodes message into binary data
func (p *Pubcomp) Encode() []byte {
	msg, err := p.encode()
	if err != nil {
		return nil
	}
	return msg.Bytes()
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Pubcomp) WriteTo(w io.Writer) (int64, error) {
	msg, err := p.encode()
	if err != nil {
		return 0, err
	}
	return msg.WriteTo(w)
}

// Type returns the Packet type.
func (p *Pubcomp) Type() MessageType {
	return MessageType(pbx.MessageType_PUBCOMP)
}

// String returns the name of operation.
func (p *Pubcomp) String() string {
	return "pubcomp"
}

// Info returns Qos and MessageID of this packet.
func (p *Pubcomp) Info() Info {
	return Info{Qos: 0, MessageID: p.MessageID}
}

func unpackPublish(data []byte) Packet {
	var pkt pbx.Publish
	proto.Unmarshal(data, &pkt)

	return &Publish{
		Topic:     pkt.Topic,
		Payload:   pkt.Payload,
		MessageID: pkt.MessageID,
		Qos:       pkt.Qos,
	}
}

func unpackPuback(data []byte) Packet {
	var pkt pbx.Puback
	proto.Unmarshal(data, &pkt)
	return &Puback{
		MessageID: pkt.MessageID,
	}
}

func unpackPubrec(data []byte) Packet {
	var pkt pbx.Pubrec
	proto.Unmarshal(data, &pkt)
	return &Pubrec{
		MessageID: pkt.MessageID,
	}
}

func unpackPubrel(data []byte) Packet {
	var pkt pbx.Pubrel
	proto.Unmarshal(data, &pkt)
	return &Pubrel{
		MessageID: pkt.MessageID,
	}
}

func unpackPubcomp(data []byte) Packet {
	var pkt pbx.Pubcomp
	proto.Unmarshal(data, &pkt)
	return &Pubcomp{
		MessageID: pkt.MessageID,
	}
}
