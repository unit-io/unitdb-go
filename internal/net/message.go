package net

import (
	"bytes"
	"fmt"
	"io"

	"github.com/unit-io/unitdb/server/utp"
)

// MessagePack is the interface for all Messages
type MessagePack interface {
	Type() utp.MessageType
	Info() utp.Info
}

// Read unpacks the packet from the provided reader.
func Read(r io.Reader) (MessagePack, error) {
	var fh utp.FixedHeader
	fh.FromBinary(r)

	// Check for empty packets
	switch fh.MessageType {
	case utp.DISCONNECT:
		return &utp.Disconnect{}, nil
	}

	rawMsg := make([]byte, fh.MessageLength)
	_, err := io.ReadFull(r, rawMsg)
	if err != nil {
		return nil, err
	}

	// unpack the body
	if fh.FlowControl != utp.NONE {
		pack := &utp.ControlMessage{}
		pack.FromBinary(fh, rawMsg)
		return pack, nil
	}
	switch fh.MessageType {
	case utp.PUBLISH:
		pack := &utp.Publish{}
		pack.FromBinary(fh, rawMsg)
		return pack, nil
	default:
		return nil, fmt.Errorf("message::Read: Invalid zero-length packet type %d", fh.MessageType)
	}
}

// Encode encodes the message into binary data
func Encode(pack MessagePack) (bytes.Buffer, error) {
	switch pack.Type() {
	case utp.PINGREQ:
		return pack.(*utp.Pingreq).ToBinary()
	case utp.CONNECT:
		return pack.(*utp.Connect).ToBinary()
	case utp.DISCONNECT:
		return pack.(*utp.Disconnect).ToBinary()
	case utp.RELAY:
		return pack.(*utp.Relay).ToBinary()
	case utp.SUBSCRIBE:
		return pack.(*utp.Subscribe).ToBinary()
	case utp.UNSUBSCRIBE:
		return pack.(*utp.Unsubscribe).ToBinary()
	case utp.PUBLISH:
		return pack.(*utp.Publish).ToBinary()
	case utp.FLOWCONTROL:
		return pack.(*utp.ControlMessage).ToBinary()
	default:
		return bytes.Buffer{}, fmt.Errorf("message::Encode: Invalid zero-length packet type %d", pack.Type())
	}
}
