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
	Pubnew      pbx.Pubnew
	Pubreceive  pbx.Pubreceive
	Pubreceipt  pbx.Pubreceipt
	Pubcomplete pbx.Pubcomplete
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
	pkt, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (p *Publish) Type() MessageType {
	return MessageType(pbx.MessageType_PUBLISH)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Publish) Info() Info {
	return Info{DeliveryMode: p.DeliveryMode, MessageID: p.MessageID}
}

// Type returns the Packet type.
func (p *Pubnew) Type() MessageType {
	return MessageType(pbx.MessageType_PUBNEW)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Pubnew) Info() Info {
	return Info{DeliveryMode: 1, MessageID: p.MessageID}
}

func encodePubreceive(p Pubreceive) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrecv := pbx.Pubreceive(p)
	pkt, err := proto.Marshal(&pubrecv)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBRECEIVE, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Pubreceive) Type() MessageType {
	return MessageType(pbx.MessageType_PUBRECEIVE)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Pubreceive) Info() Info {
	return Info{DeliveryMode: 1, MessageID: p.MessageID}
}

func encodePubreceipt(p Pubreceipt) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrec := pbx.Pubreceipt(p)
	pkt, err := proto.Marshal(&pubrec)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBRECEIPT, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Pubreceipt) Type() MessageType {
	return MessageType(pbx.MessageType_PUBRECEIPT)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Pubreceipt) Info() Info {
	return Info{DeliveryMode: 1, MessageID: p.MessageID}
}

func encodePubcomplete(p Pubcomplete) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubcomp := pbx.Pubcomplete(p)
	pkt, err := proto.Marshal(&pubcomp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBCOMPLETE, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the Packet type.
func (p *Pubcomplete) Type() MessageType {
	return MessageType(pbx.MessageType_PUBCOMPLETE)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Pubcomplete) Info() Info {
	return Info{DeliveryMode: 0, MessageID: p.MessageID}
}

func unpackPublish(data []byte) Packet {
	var pkt pbx.Publish
	proto.Unmarshal(data, &pkt)
	var msgs []*PublishMessage
	for _, m := range pkt.Messages {
		pubMsg := &PublishMessage{
			Topic:   m.Topic,
			Payload: m.Payload,
			Ttl:     m.Ttl,
		}
		msgs = append(msgs, pubMsg)
	}
	return &Publish{
		MessageID:    pkt.MessageID,
		DeliveryMode: pkt.DeliveryMode,
		Messages:     msgs,
	}
}

func unpackPubnew(data []byte) Packet {
	var pkt pbx.Pubnew
	proto.Unmarshal(data, &pkt)
	return &Pubnew{
		MessageID: pkt.MessageID,
	}
}

func unpackPubreceipt(data []byte) Packet {
	var pkt pbx.Pubreceipt
	proto.Unmarshal(data, &pkt)
	return &Pubreceipt{
		MessageID: pkt.MessageID,
	}
}

func unpackPubcomplete(data []byte) Packet {
	var pkt pbx.Pubcomplete
	proto.Unmarshal(data, &pkt)
	return &Pubcomplete{
		MessageID: pkt.MessageID,
	}
}
