package unitd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/unit-io/unitd-go/packets"
	"github.com/unit-io/unitd/pkg/log"
)

// Connect takes a connected net.Conn and performs the initial handshake. Paramaters are:
// conn - Connected net.Conn
// cm - Connect Packet
func Connect(conn net.Conn, cm *packets.Connect) (uint32, uint32, bool) {
	if _, err := cm.WriteTo(conn); err != nil {
		fmt.Println(err)
	}

	rc, cid, sessionPresent := verifyCONNACK(conn)
	return rc, cid, sessionPresent
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func verifyCONNACK(conn net.Conn) (uint32, uint32, bool) {
	ca, err := packets.ReadPacket(conn)
	if err != nil {
		return packets.ErrNetworkError, packets.ErrNetworkError, false
	}
	if ca == nil {
		return packets.ErrNetworkError, packets.ErrNetworkError, false
	}

	msg, ok := ca.(*packets.Connack)
	if !ok {
		return packets.ErrNetworkError, packets.ErrNetworkError, false
	}

	return msg.ReturnCode, msg.ConnID, true
}

// Handle handles incoming messages
func (cc *clientConn) readLoop() error {
	cc.closeW.Add(1)
	defer func() {
		log.Info("conn.Handler", "closing...")
		cc.close()
		cc.closeW.Done()
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

		// persist incoming

		// Message handler
		if err := cc.handler(pkt); err != nil {
			return err
		}
	}
}

// handle handles inbound messages.
func (cc *clientConn) handler(pkt packets.Packet) error {
	cc.updateLastAction()

	switch m := pkt.(type) {
	case *packets.Pingresp:
		cc.updateLastTouched()
	case *packets.Suback:
		mId := cc.inboundID(m.MessageID)
		cc.getType(mId).flowComplete()
		cc.freeID(mId)
	case *packets.Unsuback:
		mId := cc.inboundID(m.MessageID)
		cc.getType(mId).flowComplete()
		cc.freeID(mId)
	case *packets.Publish:
		// mId := cc.inboundID(m.MessageID)
		// cc.freeID(mId)
		cc.recv <- pkt.(*packets.Publish)
	case *packets.Puback:
		mId := cc.inboundID(m.MessageID)
		cc.getType(mId).flowComplete()
		cc.freeID(mId)
	case *packets.Pubrec:
		p := packets.Packet(&packets.Pubrel{MessageID: m.MessageID})
		cc.send <- &PacketAndResult{p: p}
	case *packets.Pubrel:
		p := packets.Packet(&packets.Pubcomp{MessageID: m.MessageID})
		cc.send <- &PacketAndResult{p: p}
	case *packets.Pubcomp:
		mId := cc.inboundID(m.MessageID)
		cc.getType(mId).flowComplete()
		cc.freeID(mId)
	}

	return nil
}

func (cc *clientConn) writeLoop(ctx context.Context) {
	cc.closeW.Add(1)
	defer cc.closeW.Done()

	for {
		select {
		case <-ctx.Done():
			return
		// case <-cc.closeC:
		// 	return
		case msg, ok := <-cc.send:
			if !ok {
				// Channel closed.
				return
			}
			switch m := msg.p.(type) {
			case *packets.Publish:
				if m.Qos == 0 {
					msg.r.(*PublishResult).flowComplete()
					mId := cc.inboundID(m.MessageID)
					// cc.getType(mId).flowComplete()
					cc.freeID(mId)
				}
			case *packets.Puback:
				// persist outbound
			case *packets.Pubrel:
				// persist outbound
			case *packets.Disconnect:
				msg.r.(*DisconnectResult).flowComplete()
				mId := cc.inboundID(m.MessageID)
				// cc.getType(mId).flowComplete()
				cc.freeID(mId)
			}
			msg.p.WriteTo(cc.conn)
		}
	}
}

func (cc *clientConn) dispatcher(ctx context.Context) {
	cc.closeW.Add(1)
	defer cc.closeW.Done()

	for {
		select {
		case <-ctx.Done():
			return
		// case <-cc.closeC:
		// 	return
		case msg, ok := <-cc.recv:
			if !ok {
				// Channel closed.
				return
			}
			m := messageFromPublish(msg, ack(cc, msg))
			// dispatch message to default callback function
			handler := cc.callbacks[0]
			handler(cc, m)
		}
	}
}

// keepalive - Send ping when connection unused for set period
// connection passed in to avoid race condition on shutdown
func (cc *clientConn) keepalive(ctx context.Context) {
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
		case <-ctx.Done():
			return
		// case <-cc.closeC:
		// 	return
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

// ack acknowledges a packet
func ack(cc *clientConn, packet *packets.Publish) func() {
	return func() {
		switch packet.Qos {
		case 2:
			p := packets.Packet(&packets.Pubrec{MessageID: packet.MessageID})
			cc.send <- &PacketAndResult{p: p}
		case 1:
			p := packets.Packet(&packets.Puback{MessageID: packet.MessageID})
			cc.send <- &PacketAndResult{p: p}
		case 0:
			// do nothing, since there is no need to send an ack packet back
		}
	}
}
