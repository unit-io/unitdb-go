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
	m, err := packets.Encode(cm)
	if err != nil {
		fmt.Println(err)
	}
	conn.Write(m.Bytes())
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
func (c *client) readLoop(ctx context.Context) error {
	// c.closeW.Add(1)
	defer func() {
		defer log.Info("conn.Handler", "closing...")
		// c.closeW.Done()
	}()

	reader := bufio.NewReaderSize(c.conn, 65536)

	for {
		// Set read/write deadlines so we can close dangling connections
		c.conn.SetDeadline(time.Now().Add(time.Second * 120))

		select {
		case <-ctx.Done():
			return nil
		case <-c.closeC:
			return nil
		default:
			// Decode an incoming packet
			msg, err := packets.ReadPacket(reader)
			if err != nil {
				return err
			}

			// Persist incoming
			c.storeInbound(msg)

			// Message handler
			if err := c.handler(msg); err != nil {
				return err
			}
		}
	}
}

// handle handles inbound messages.
func (c *client) handler(msg packets.Packet) error {
	c.updateLastAction()

	switch m := msg.(type) {
	case *packets.Pingresp:
		c.updateLastTouched()
	case *packets.Suback:
		mId := c.inboundID(m.MessageID)
		c.getType(mId).flowComplete()
		c.freeID(mId)
	case *packets.Unsuback:
		mId := c.inboundID(m.MessageID)
		c.getType(mId).flowComplete()
		c.freeID(mId)
	case *packets.Publish:
		c.pub <- msg.(*packets.Publish)
	case *packets.Puback:
		mId := c.inboundID(m.MessageID)
		c.getType(mId).flowComplete()
		c.freeID(mId)
	case *packets.Pubrec:
		p := packets.Packet(&packets.Pubrel{MessageID: m.MessageID, Qos: m.Qos})
		c.send <- &PacketAndResult{p: p}
	case *packets.Pubrel:
		p := packets.Packet(&packets.Pubcomp{MessageID: m.MessageID})
		// persist outbound
		c.storeOutbound(p)
		c.send <- &PacketAndResult{p: p}
	case *packets.Pubcomp:
		mId := c.inboundID(m.MessageID)
		c.getType(mId).flowComplete()
		c.freeID(mId)
	}

	return nil
}

func (c *client) writeLoop(ctx context.Context) {
	// c.closeW.Add(1)
	// defer c.closeW.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case msg, ok := <-c.send:
			if !ok {
				// Channel closed.
				return
			}
			switch m := msg.p.(type) {
			case *packets.Publish:
				if m.Qos == 0 {
					msg.r.(*PublishResult).flowComplete()
					mId := c.inboundID(m.MessageID)
					c.freeID(mId)
				}
			case *packets.Disconnect:
				msg.r.(*DisconnectResult).flowComplete()
				mId := c.inboundID(m.MessageID)
				c.freeID(mId)
			}
			m, err := packets.Encode(msg.p)
			if err != nil {
				fmt.Println(err)
				return
			}
			c.conn.Write(m.Bytes())
		}
	}
}

func (c *client) dispatcher(ctx context.Context) {
	// c.closeW.Add(1)
	// defer c.closeW.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case msg, ok := <-c.pub:
			if !ok {
				// Channel closed.
				return
			}
			m := messageFromPublish(msg, ack(c, msg))
			// dispatch message to default callback function
			handler := c.callbacks[0]
			handler(c, m)
		}
	}
}

// keepalive - Send ping when connection unused for set period
// connection passed in to avoid race condition on shutdown
func (c *client) keepalive(ctx context.Context) {
	// c.closeW.Add(1)
	// defer c.closeW.Done()

	var pingInterval int64
	var pingSent time.Time

	if c.opts.keepAlive > 10 {
		pingInterval = 5
	} else {
		pingInterval = c.opts.keepAlive / 2
	}

	pingTicker := time.NewTicker(time.Duration(pingInterval * int64(time.Second)))
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case <-pingTicker.C:
			lastAction := c.lastAction.Load().(time.Time)
			lastTouched := c.lastTouched.Load().(time.Time)
			live := TimeNow().Add(-time.Duration(c.opts.keepAlive * int64(time.Second)))
			timeout := TimeNow().Add(-c.opts.pingTimeout)

			if lastAction.After(live) && lastTouched.Before(timeout) {
				ping := &packets.Pingreq{}
				m, err := packets.Encode(ping)
				if err != nil {
					fmt.Println(err)
				}
				c.conn.Write(m.Bytes())
				pingSent = TimeNow()
			}
			if lastTouched.Before(timeout) && pingSent.Before(timeout) {
				fmt.Println("pingresp not received, disconnecting")
				go c.internalConnLost(errors.New("pingresp not received, disconnecting")) // no harm in calling this if the connection is already down (better than stopping!)
				return
			}
		}
	}
}

// ack acknowledges a packet
func ack(c *client, packet *packets.Publish) func() {
	return func() {
		switch packet.Qos {
		case 2:
			p := packets.Packet(&packets.Pubrec{MessageID: packet.MessageID, Qos: packet.Qos})
			c.send <- &PacketAndResult{p: p}
		case 1:
			p := packets.Packet(&packets.Puback{MessageID: packet.MessageID})
			// persist outbound
			c.storeOutbound(p)
			c.send <- &PacketAndResult{p: p}
		case 0:
			// do nothing, since there is no need to send an ack packet back
		}
	}
}
