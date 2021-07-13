package utp

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type (
	RelayRequest pbx.RelayRequest
	Relay        struct {
		MessageID     int32
		RelayRequests []*RelayRequest
	}
)

func encodeRelay(r Relay) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var reqs []*pbx.RelayRequest
	for _, req := range r.RelayRequests {
		r := pbx.RelayRequest(*req)
		reqs = append(reqs, &r)
	}
	rel := pbx.Relay{
		MessageID:     r.MessageID,
		RelayRequests: reqs,
	}
	rawMsg, err := proto.Marshal(&rel)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_RELAY, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

// Type returns the Message type.
func (r *Relay) Type() MessageType {
	return RELAY
}

// Info returns DeliveryMode and MessageID of this Message.
func (r *Relay) Info() Info {
	return Info{DeliveryMode: 1, MessageID: r.MessageID}
}
