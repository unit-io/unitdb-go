package utp

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

// FlowControl represents FlowControl Message type
type FlowControl uint8

const (
	// Flow Control
	NONE FlowControl = iota
	ACKNOWLEDGE
	NOTIFY
	RECEIVE
	RECEIPT
	COMPLETE
)

func (c FlowControl) Value() uint8 {
	return uint8(c)
}

type ControlMessage struct {
	MessageID   int32
	MessageType MessageType
	FlowControl FlowControl
	Message     Message
}

// encodeControlMessage encodes the Control Message into binary data
func encodeControlMessage(c ControlMessage) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var err error
	var fh FixedHeader
	ctrl := pbx.ControlMessage{
		MessageID: int32(c.MessageID),
	}
	rawMsg, err := proto.Marshal(&ctrl)
	if err != nil {
		return msg, err
	}
	switch c.FlowControl {
	case ACKNOWLEDGE:
		switch c.MessageType {
		case CONNECT:
			connack := *c.Message.(*ConnectAcknowledge)
			ack := pbx.ConnectAcknowledge{
				ReturnCode: int32(connack.ReturnCode),
				Epoch:      int32(connack.Epoch),
				ConnID:     int32(connack.ConnID),
			}
			rawAck, err := proto.Marshal(&ack)
			if err != nil {
				return msg, err
			}
			ctrl.Message = rawAck
			rawMsg, err := proto.Marshal(&ctrl)
			if err != nil {
				return msg, err
			}
			fh = FixedHeader{MessageType: pbx.MessageType_CONNECT, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case PUBLISH:
			fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case RELAY:
			fh = FixedHeader{MessageType: pbx.MessageType_RELAY, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case SUBSCRIBE:
			fh = FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case UNSUBSCRIBE:
			fh = FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case PINGREQ:
			fh = FixedHeader{MessageType: pbx.MessageType_PINGREQ, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		}
	case NOTIFY:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_NOTIFY, MessageLength: int32(len(rawMsg))}
	case RECEIVE:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_RECEIVE, MessageLength: int32(len(rawMsg))}
	case RECEIPT:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_RECEIPT, MessageLength: int32(len(rawMsg))}
	case COMPLETE:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_COMPLETE, MessageLength: int32(len(rawMsg))}
	}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func unpackControlMessage(h FixedHeader, data []byte) Message {
	var ctrl pbx.ControlMessage
	proto.Unmarshal(data, &ctrl)

	switch uint8(h.MessageType) {
	case CONNECT.Value():
		var connack pbx.ConnectAcknowledge
		proto.Unmarshal(ctrl.Message, &connack)

		return &ConnectAcknowledge{
			ReturnCode: connack.ReturnCode,
			Epoch:      connack.Epoch,
			ConnID:     connack.ConnID,
		}
	}
	return &ControlMessage{MessageID: ctrl.MessageID, MessageType: MessageType(h.MessageType), FlowControl: FlowControl(h.FlowControl)}
}

// Type returns the Message type.
func (c *ControlMessage) Type() MessageType {
	return FLOWCONTROL
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *ControlMessage) Info() Info {
	return Info{DeliveryMode: 1, MessageID: c.MessageID}
}
