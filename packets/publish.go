package packets

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type (
	Publish pbx.Publish
	Puback  pbx.Puback
	Pubrec  pbx.Pubrec
	Pubrel  pbx.Pubrel
	Pubcomp pbx.Pubcomp
)

func encodePublish(p Publish) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pub := pbx.Publish(p)
	pkt, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (p *Publish) Type() MessageType {
	return MessageType(pbx.MessageType_PUBLISH)
}

// Info returns Qos and MessageID of this packet.
func (p *Publish) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

func encodePuback(p Puback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	puback := pbx.Puback(p)
	pkt, err := proto.Marshal(&puback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Puback) Type() MessageType {
	return MessageType(pbx.MessageType_PUBACK)
}

// Info returns Qos and MessageID of this packet.
func (p *Puback) Info() Info {
	return Info{Qos: 0, MessageID: p.MessageID}
}

func encodePubrec(p Pubrec) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrec := pbx.Pubrec(p)
	pkt, err := proto.Marshal(&pubrec)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREC, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Pubrec) Type() MessageType {
	return MessageType(pbx.MessageType_PUBREC)
}

// Info returns Qos and MessageID of this packet.
func (p *Pubrec) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

func encodePubrel(p Pubrel) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrel := pbx.Pubrel(p)
	pkt, err := proto.Marshal(&pubrel)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREL, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Pubrel) Type() MessageType {
	return MessageType(pbx.MessageType_PUBREL)
}

// Info returns Qos and MessageID of this packet.
func (p *Pubrel) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

func encodePubcomp(p Pubcomp) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubcomp := pbx.Pubcomp(p)
	pkt, err := proto.Marshal(&pubcomp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBCOMP, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Pubcomp) Type() MessageType {
	return MessageType(pbx.MessageType_PUBCOMP)
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
		Qos:       pkt.Qos,
	}
}

func unpackPubrel(data []byte) Packet {
	var pkt pbx.Pubrel
	proto.Unmarshal(data, &pkt)
	return &Pubrel{
		MessageID: pkt.MessageID,
		Qos:       pkt.Qos,
	}
}

func unpackPubcomp(data []byte) Packet {
	var pkt pbx.Pubcomp
	proto.Unmarshal(data, &pkt)
	return &Pubcomp{
		MessageID: pkt.MessageID,
	}
}
