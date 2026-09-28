package net

import (
	"bytes"
	"testing"

	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
	"google.golang.org/protobuf/proto"
)

func TestReadRejectsInvalidLength(t *testing.T) {
	for _, length := range []int32{-1, -1 << 30, MaxFrameSize + 1} {
		h, err := proto.Marshal(&pbx.FixedHeader{MessageType: pbx.MessageType(utp.PUBLISH), MessageLength: length})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Read(bytes.NewReader(append([]byte{byte(len(h))}, h...))); err == nil {
			t.Errorf("length %d: expected an error", length)
		}
	}
}
