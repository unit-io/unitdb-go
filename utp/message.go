package utp

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

// MessageType represents a Message type
type MessageType uint8

const (
	// Message
	CONNECT MessageType = iota + 1
	PUBLISH
	SUBSCRIBE
	UNSUBSCRIBE
	PINGREQ
	DISCONNECT
	FLOWCONTROL
)

// Below are the const definitions for error codes returned by
// Connect()
const (
	Accepted                      = 0x00
	ErrRefusedBadProtocolVersion  = 0x01
	ErrRefusedIDRejected          = 0x02
	ErrRefusedbADID               = 0x03
	ErrRefusedServerUnavailable   = 0x04
	ErrNotAuthorised              = 0x05
	ErrBadRequest                 = 0x06
)

func (t MessageType) Value() uint8 {
	return uint8(t)
}

// Message is the interface all Messages
type Message interface {
	Type() MessageType
	Info() Info
}

type FixedHeader pbx.FixedHeader

// Info returns Qos and MessageID by the Info() function called on the Packet
type Info struct {
	DeliveryMode int32
	MessageID    int32
}

// Read unpacks the packet from the provided reader.
func Read(r io.Reader) (Message, error) {
	var fh FixedHeader
	fh.unpack(r)

	// Check for empty packets
	switch uint8(fh.MessageType) {
	case DISCONNECT.Value():
		return &Disconnect{}, nil
	}

	rawMsg := make([]byte, fh.MessageLength)
	_, err := io.ReadFull(r, rawMsg)
	if err != nil {
		return nil, err
	}

	// unpack the body
	var msg Message
	if uint8(fh.FlowControl) != NONE.Value() {
		return unpackControlMessage(fh, rawMsg), nil
	}
	switch uint8(fh.MessageType) {
	case PUBLISH.Value():
		msg = unpackPublish(rawMsg)
	default:
		return nil, fmt.Errorf("message::Read: Invalid zero-length packet type %d", fh.MessageType)
	}

	return msg, nil
}

// Encode encodes the message into binary data
func Encode(msg Message) (bytes.Buffer, error) {
	switch msg.Type() {
	case PINGREQ:
		return encodePingreq(*msg.(*Pingreq))
	case CONNECT:
		return encodeConnect(*msg.(*Connect))
	case DISCONNECT:
		return encodeDisconnect(*msg.(*Disconnect))
	case SUBSCRIBE:
		return encodeSubscribe(*msg.(*Subscribe))
	case UNSUBSCRIBE:
		return encodeUnsubscribe(*msg.(*Unsubscribe))
	case PUBLISH:
		return encodePublish(*msg.(*Publish))
	case FLOWCONTROL:
		return encodeControlMessage(*msg.(*ControlMessage))
	default:
		return bytes.Buffer{}, fmt.Errorf("message::Encode: Invalid zero-length packet type %d", msg.Type())
	}
	return bytes.Buffer{}, nil
}

func (fh *FixedHeader) pack() bytes.Buffer {
	var head bytes.Buffer
	ph := pbx.FixedHeader(*fh)
	h, err := proto.Marshal(&ph)
	if err != nil {
		return head
	}
	size := encodeLength(len(h))
	head.Write(size)
	head.Write(h)
	return head
}

func (fh *FixedHeader) unpack(r io.Reader) error {
	fhSize, err := decodeLength(r)
	if err != nil {
		return err
	}

	// read FixedHeader
	head := make([]byte, fhSize)
	_, err = io.ReadFull(r, head)
	if err != nil {
		return err
	}

	var h pbx.FixedHeader
	proto.Unmarshal(head, &h)

	*fh = FixedHeader(h)
	return nil
}

func encodeLength(length int) []byte {
	var encLength []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		encLength = append(encLength, digit)
		if length == 0 {
			break
		}
	}
	return encLength
}

func decodeLength(r io.Reader) (int, error) {
	var rLength uint32
	var multiplier uint32
	b := make([]byte, 1)
	for multiplier < 27 { //fix: Infinite '(digit & 128) == 1' will cause the dead loop
		_, err := io.ReadFull(r, b)
		if err != nil {
			return 0, err
		}

		digit := b[0]
		rLength |= uint32(digit&127) << multiplier
		if (digit & 128) == 0 {
			break
		}
		multiplier += 7
	}
	return int(rLength), nil
}
