package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitd/proto"
)

const (
	CONNECT = uint8(iota + 1)
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
)

//Below are the const definitions for error codes returned by
//Connect()
const (
	Accepted                        = 0x00
	ErrRefusedBadProtocolVersion    = 0x01
	ErrRefusedIDRejected            = 0x02
	ErrRefusedServerUnavailable     = 0x03
	ErrRefusedBadUsernameOrPassword = 0x04
	ErrRefusedNotAuthorised         = 0x05
	ErrNetworkError                 = 0xFE
	ErrProtocolViolation            = 0xFF
)

//Packet is the interface all our packets in the line protocol will be implementing
type Packet interface {
	Type() MessageType
	Info() Info
}

type MessageType pbx.MessageType
type FixedHeader pbx.FixedHeader

// Info returns Qos and MessageID by the Info() function called on the Packet
type Info struct {
	Qos       uint32
	MessageID uint32
}

// ReadPacket unpacks the packet from the provided reader.
func ReadPacket(r io.Reader) (Packet, error) {
	var fh FixedHeader
	fh.unpack(r)

	// Check for empty packets
	switch uint8(fh.MessageType) {
	case PINGREQ:
		return &Pingreq{}, nil
	case PINGRESP:
		return &Pingresp{}, nil
	case DISCONNECT:
		return &Disconnect{}, nil
	}

	msg := make([]byte, fh.RemainingLength)
	_, err := io.ReadFull(r, msg)
	if err != nil {
		return nil, err
	}

	// unpack the body
	var pkt Packet
	switch uint8(fh.MessageType) {
	case CONNECT:
		pkt = unpackConnect(msg)
	case CONNACK:
		pkt = unpackConnack(msg)
	case PUBLISH:
		pkt = unpackPublish(msg)
	case PUBACK:
		pkt = unpackPuback(msg)
	case PUBREC:
		pkt = unpackPubrec(msg)
	case PUBREL:
		pkt = unpackPubrel(msg)
	case PUBCOMP:
		pkt = unpackPubcomp(msg)
	case SUBSCRIBE:
		pkt = unpackSubscribe(msg)
	case SUBACK:
		pkt = unpackSuback(msg)
	case UNSUBSCRIBE:
		pkt = unpackUnsubscribe(msg)
	case UNSUBACK:
		pkt = unpackUnsuback(msg)
	default:
		return nil, fmt.Errorf("Invalid zero-length packet with type %d", fh.MessageType)
	}

	return pkt, nil
}

// Encode encodes the message into binary data
func Encode(pkt Packet) (bytes.Buffer, error) {
	switch uint8(pkt.Type()) {
	case PINGREQ:
		return encodePingreq(*pkt.(*Pingreq))
	case PINGRESP:
		return encodePingresp(*pkt.(*Pingresp))
	case CONNECT:
		return encodeConnect(*pkt.(*Connect))
	case CONNACK:
		return encodeConnack(*pkt.(*Connack))
	case DISCONNECT:
		return encodeDisconnect(*pkt.(*Disconnect))
	case SUBSCRIBE:
		return encodeSubscribe(*pkt.(*Subscribe))
	case SUBACK:
		return encodeSuback(*pkt.(*Suback))
	case UNSUBSCRIBE:
		return encodeUnsubscribe(*pkt.(*Unsubscribe))
	case UNSUBACK:
		return encodeUnsuback(*pkt.(*Unsuback))
	case PUBLISH:
		return encodePublish(*pkt.(*Publish))
	case PUBACK:
		return encodePuback(*pkt.(*Puback))
	case PUBREC:
		return encodePubrec(*pkt.(*Pubrec))
	case PUBREL:
		return encodePubrel(*pkt.(*Pubrel))
	case PUBCOMP:
		return encodePubcomp(*pkt.(*Pubcomp))
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
