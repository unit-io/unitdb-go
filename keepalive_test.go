package unitdb

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
)

// silentServer accepts one connection, accepts its CONNECT and then never
// answers again.
func silentServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		var fh utp.FixedHeader
		if err := fh.FromBinary(r); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, r, int64(fh.MessageLength)); err != nil {
			return
		}
		ack, _ := (&utp.ConnectAcknowledge{ReturnCode: utp.Accepted, Epoch: 1, ConnID: 1000}).ToBinary()
		buf, _ := lp.Encode(&utp.ControlMessage{MessageType: utp.CONNECT, FlowControl: utp.ACKNOWLEDGE, Message: ack.Bytes()})
		conn.Write(buf.Bytes())
		io.Copy(io.Discard, r)
	}()
	return l.Addr().String()
}

func TestKeepaliveDetectsSilentServer(t *testing.T) {
	lost := make(chan error, 1)
	c, err := NewClient("tcp://"+silentServer(t), "",
		WithStorePath(storeDir(t)),
		WithKeepAlive(2*time.Second),
		WithPingTimeout(time.Second),
		WithConnectionLostHandler(func(_ Client, err error) { lost <- err }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	// Idle for the keep alive, one ping interval, then the ping timeout.
	select {
	case err := <-lost:
		if err == nil {
			t.Fatal("connection lost without an error")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("unanswered pings were not detected")
	}
}

func TestKeepaliveOneSecond(t *testing.T) {
	c, err := NewClient("tcp://"+silentServer(t), "",
		WithStorePath(storeDir(t)),
		WithKeepAlive(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	// A one second keep alive used to create a zero ping interval and panic.
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	c.Disconnect()
}
