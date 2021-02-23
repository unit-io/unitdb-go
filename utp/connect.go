package utp

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type (
	Connect    pbx.Conn
	Connack    pbx.Connack
	Pingreq    pbx.Pingreq
	Pingresp   pbx.Pingresp
	Disconnect pbx.Disconnect
)

func encodeConnect(c Connect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	conn := pbx.Conn(c)
	pkt, err := proto.Marshal(&conn)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_CONNECT, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (c *Connect) Type() MessageType {
	return MessageType(pbx.MessageType_CONNECT)
}

// Info returns DeliveryMode and MessageID of this packet.
func (c *Connect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func encodeConnack(c Connack) (bytes.Buffer, error) {
	var msg bytes.Buffer
	connack := pbx.Connack(c)
	pkt, err := proto.Marshal(&connack)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_CONNACK, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (c *Connack) Type() MessageType {
	return MessageType(pbx.MessageType_CONNACK)
}

// Info returns DeliveryMode and MessageID of this packet.
func (c *Connack) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func encodePingreq(p Pingreq) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pingreq := pbx.Pingreq(p)
	pkt, err := proto.Marshal(&pingreq)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PINGREQ, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (p *Pingreq) Type() MessageType {
	return MessageType(pbx.MessageType_PINGREQ)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Pingreq) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func encodePingresp(p Pingresp) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pingresp := pbx.Pingresp(p)
	pkt, err := proto.Marshal(&pingresp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PINGRESP, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (p *Pingresp) Type() MessageType {
	return MessageType(pbx.MessageType_PINGRESP)
}

// Info returns DeliveryMode and MessageID of this packet.
func (p *Pingresp) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func encodeDisconnect(d Disconnect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	disc := pbx.Disconnect(d)
	pkt, err := proto.Marshal(&disc)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_DISCONNECT, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

// Type returns the packet type.
func (d *Disconnect) Type() MessageType {
	return MessageType(pbx.MessageType_DISCONNECT)
}

// Info returns DeliveryMode and MessageID of this packet.
func (d *Disconnect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func unpackConnect(data []byte) Packet {
	var pkt pbx.Conn
	proto.Unmarshal(data, &pkt)

	connect := &Connect{
		ProtoName:     pkt.ProtoName,
		Version:       int32(pkt.Version),
		KeepAlive:     int32(pkt.KeepAlive),
		ClientID:      pkt.ClientID,
		InsecureFlag:  pkt.InsecureFlag,
		Username:      pkt.Username,
		Password:      pkt.Password,
		CleanSessFlag: pkt.CleanSessFlag,
	}

	return connect
}

func unpackConnack(data []byte) Packet {
	var pkt pbx.Connack
	proto.Unmarshal(data, &pkt)

	return &Connack{
		ReturnCode: pkt.ReturnCode,
		ConnID:     pkt.ConnID,
	}
}
