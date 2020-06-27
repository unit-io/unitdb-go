package packets

import (
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

// WriteTo writes the encoded message to the buffer.
func (p *Publish) WriteTo(w io.Writer) (int64, error) {
	pub := pbx.Publish(*p)
	pkt, err := proto.Marshal(&pub)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the packet type.
func (p *Publish) Type() MessageType {
	return MessageType(pbx.MessageType_PUBLISH)
}

// String returns the name of operation.
func (p *Publish) String() string {
	return "pub"
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Puback) WriteTo(w io.Writer) (int64, error) {
	puback := pbx.Puback(*p)
	pkt, err := proto.Marshal(&puback)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBACK, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (p *Puback) Type() MessageType {
	return MessageType(pbx.MessageType_PUBACK)
}

// String returns the name of operation.
func (p *Puback) String() string {
	return "puback"
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Pubrec) WriteTo(w io.Writer) (int64, error) {
	pubrec := pbx.Pubrec(*p)
	pkt, err := proto.Marshal(&pubrec)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREC, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (p *Pubrec) Type() MessageType {
	return MessageType(pbx.MessageType_PUBREC)
}

// String returns the name of operation.
func (p *Pubrec) String() string {
	return "pubrec"
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Pubrel) WriteTo(w io.Writer) (int64, error) {
	pubrel := pbx.Pubrel(*p)
	pkt, err := proto.Marshal(&pubrel)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREL, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (p *Pubrel) Type() MessageType {
	return MessageType(pbx.MessageType_PUBREL)
}

// String returns the name of operation.
func (p *Pubrel) String() string {
	return "pubrel"
}

// WriteTo writes the encoded Packet to the underlying writer.
func (p *Pubcomp) WriteTo(w io.Writer) (int64, error) {
	pubcomp := pbx.Pubcomp(*p)
	pkt, err := proto.Marshal(&pubcomp)
	if err != nil {
		return 0, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBCOMP, RemainingLength: uint32(len(pkt))}
	buf := fh.pack()
	buf.Write(pkt)
	return buf.WriteTo(w)
}

// Type returns the Packet type.
func (p *Pubcomp) Type() MessageType {
	return MessageType(pbx.MessageType_PUBCOMP)
}

// String returns the name of operation.
func (p *Pubcomp) String() string {
	return "pubcomp"
}

func unpackPublish(data []byte) Packet {
	var pkt pbx.Publish
	proto.Unmarshal(data, &pkt)

	return &Publish{
		Topic:     pkt.Topic,
		Payload:   pkt.Payload,
		MessageID: pkt.MessageID,
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
