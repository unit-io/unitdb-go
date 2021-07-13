package utp

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type (
	Connect    pbx.Connect
	ConnectAcknowledge    pbx.ConnectAcknowledge
	Pingreq    pbx.PingRequest
	Disconnect pbx.Disconnect
)

func encodeConnect(c Connect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	conn := pbx.Connect(c)
	rawMsg, err := proto.Marshal(&conn)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_CONNECT, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (c *Connect) Type() MessageType {
	return CONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *Connect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Message type.
func (c *ConnectAcknowledge) Type() MessageType {
	return FLOWCONTROL
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *ConnectAcknowledge) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func encodePingreq(p Pingreq) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pingreq := pbx.PingRequest(p)
	rawMsg, err := proto.Marshal(&pingreq)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PINGREQ, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (p *Pingreq) Type() MessageType {
	return PINGREQ
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pingreq) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func encodeDisconnect(d Disconnect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	disc := pbx.Disconnect(d)
	rawMsg, err := proto.Marshal(&disc)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_DISCONNECT, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (d *Disconnect) Type() MessageType {
	return DISCONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (d *Disconnect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}
