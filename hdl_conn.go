package unitd

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/unit-io/unitd-go/packets"
	"github.com/unit-io/unitd/pkg/log"
	pbx "github.com/unit-io/unitd/proto"
)

// Connect takes a connected net.Conn and performs the initial handshake. Paramaters are:
// conn - Connected net.Conn
// cm - Connect Packet
func Connect(conn net.Conn, cm *packets.Connect) (uint32, bool) {
	if _, err := cm.WriteTo(conn); err != nil {
		fmt.Println(err)
	}

	rc, sessionPresent := verifyCONNACK(conn)
	return rc, sessionPresent
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func verifyCONNACK(conn net.Conn) (uint32, bool) {
	ca, err := packets.ReadPacket(conn)
	if err != nil {
		return packets.ErrNetworkError, false
	}
	if ca == nil {
		return packets.ErrNetworkError, false
	}

	msg, ok := ca.(*packets.Connack)
	if !ok {
		return packets.ErrNetworkError, false
	}

	return msg.ReturnCode, true
}

// Handle handles incoming request to the client
func (cc *clientConn) readLoop() error {
	defer func() {
		log.Info("conn.Handler", "closing...")
		cc.close()
	}()

	reader := bufio.NewReaderSize(cc.conn, 65536)

	for {
		// Set read/write deadlines so we can close dangling connections
		cc.conn.SetDeadline(time.Now().Add(time.Second * 120))

		// Decode an incoming packet
		pkt, err := packets.ReadPacket(reader)
		if err != nil {
			return err
		}

		// Mqtt message handler
		if err := cc.handler(pkt); err != nil {
			return err
		}
	}
}

// handle handles receive.
func (cc *clientConn) handler(pkt packets.Packet) error {
	cc.updateLastAction()

	switch pkt.Type() {
	// An attempt to connect.
	case pbx.MessageType_PINGRESP:
		cc.updateLastTouched()
		// An attempt to subscribe to a topic.
		return nil
	case pbx.MessageType_SUBACK:
		return nil
	// An attempt to unsubscribe from a topic.
	case pbx.MessageType_UNSUBACK:
		return nil
	// Ping response, respond appropriately.
	case pbx.MessageType_PUBLISH:
		cc.recv <- pkt.(*packets.Publish)
		return nil
	case pbx.MessageType_PUBACK:
		return nil
	case pbx.MessageType_PUBREC:
		return nil
	case pbx.MessageType_PUBREL:
		return nil
	case pbx.MessageType_PUBCOMP:
		return nil
	}

	return nil
}

// sendMessage forwards the message to the underlying client.
func (cc *clientConn) sendMessage(m pbx.Publish) error {
	packet := packets.Publish{
		MessageID: 0,         // TODO
		Topic:     m.Topic,   // The topic for this message.
		Payload:   m.Payload, // The payload for this message.
	}

	// Acknowledge the publication
	if _, err := packet.WriteTo(cc.conn); err != nil {
		return err
	}

	return nil
}

// Send forwards raw bytes to the underlying client.
func (cc *clientConn) sendRawBytes(buf []byte) bool {
	if cc.conn == nil {
		return true
	}

	select {
	case cc.send <- buf:
	case <-time.After(time.Millisecond * 5):
		return false
	}

	return true
}

func (cc *clientConn) writeLoop() {
	cc.closeW.Add(1)
	defer cc.closeW.Done()

	for {
		select {
		case <-cc.closeC:
			return
		case msg, ok := <-cc.send:
			if !ok {
				// Channel closed.
				return
			}
			cc.conn.Write(msg)
		}
	}
}

func (cc *clientConn) dispatcher() {
	cc.closeW.Add(1)
	defer cc.closeW.Done()

	for {
		select {
		case <-cc.closeC:
			return
		case msg, ok := <-cc.recv:
			if !ok {
				// Channel closed.
				return
			}
			m := messageFromPublish(msg)
			// dispatch message to default callback function
			handler := cc.callbacks[0]
			handler(cc, m)
		}
	}
}

// keepalive - Send ping when connection unused for set period
// connection passed in to avoid race condition on shutdown
func (cc *clientConn) keepalive() {
	cc.closeW.Add(1)
	defer cc.closeW.Done()

	var pingInterval int64
	var pingSent time.Time

	if cc.opts.KeepAlive > 10 {
		pingInterval = 5
	} else {
		pingInterval = cc.opts.KeepAlive / 2
	}

	pingTicker := time.NewTicker(time.Duration(pingInterval * int64(time.Second)))
	defer pingTicker.Stop()

	for {
		select {
		case <-cc.closeC:
			return
		case <-pingTicker.C:
			lastAction := cc.lastAction.Load().(time.Time)
			lastTouched := cc.lastTouched.Load().(time.Time)
			live := TimeNow().Add(-time.Duration(cc.opts.KeepAlive * int64(time.Second)))
			timeout := TimeNow().Add(-cc.opts.PingTimeout)

			if lastAction.After(live) && lastTouched.Before(timeout) {
				ping := packets.Pingreq{}
				if _, err := ping.WriteTo(cc.conn); err != nil {
					fmt.Println(err)
				}
				pingSent = TimeNow()
			}
			if lastTouched.Before(timeout) && pingSent.Before(timeout) {
				fmt.Println("pingresp not received, disconnecting")
				go cc.internalConnLost(errors.New("pingresp not received, disconnecting")) // no harm in calling this if the connection is already down (better than stopping!)
				return
			}
		}
	}
}
