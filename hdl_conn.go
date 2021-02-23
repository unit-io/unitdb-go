package unitdb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/unit-io/unitdb-go/utp"
)

// Connect takes a connected net.Conn and performs the initial handshake. Paramaters are:
// conn - Connected net.Conn
// cm - Connect Packet
func Connect(conn net.Conn, cm *utp.Connect) (int32, int32, bool) {
	m, err := utp.Encode(cm)
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
func verifyCONNACK(conn net.Conn) (int32, int32, bool) {
	ca, err := utp.ReadPacket(conn)
	if err != nil {
		return utp.ErrNetworkError, utp.ErrNetworkError, false
	}
	if ca == nil {
		return utp.ErrNetworkError, utp.ErrNetworkError, false
	}

	msg, ok := ca.(*utp.Connack)
	if !ok {
		return utp.ErrNetworkError, utp.ErrNetworkError, false
	}

	return msg.ReturnCode, msg.ConnID, true
}

// Handle handles incoming messages
func (c *client) readLoop(ctx context.Context) error {
	defer func() {
		defer log.Println("conn.Handler: closing...")
	}()

	reader := bufio.NewReaderSize(c.conn, 65536)
	// Set read/write deadlines so we can close dangling connections
	c.conn.SetDeadline(time.Now().Add(time.Second * 120))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.closeC:
			return nil
		default:
			// Decode an incoming packet
			msg, err := utp.ReadPacket(reader)
			if err != nil {
				fmt.Println("conn::readLoop: read packet error ", err)
				return err
			}

			// Persist incoming
			c.storeInbound(msg)

			// Message handler
			if err := c.handler(msg); err != nil {
				fmt.Println("conn::readLoop: message handler error ", err)
				return err
			}
		}
	}
}

// handle handles inbound messages.
func (c *client) handler(msg utp.Packet) error {
	c.updateLastAction()

	switch m := msg.(type) {
	case *utp.Pingresp:
		c.updateLastTouched()
		fmt.Println("conn::handler: pingresp ", c.lastTouched.Load().(time.Time))
	case *utp.Suback:
		mId := c.inboundID(m.MessageID)
		c.getType(mId).flowComplete()
		c.freeID(mId)
	case *utp.Unsuback:
		mId := c.inboundID(m.MessageID)
		c.getType(mId).flowComplete()
		c.freeID(mId)
	case *utp.Publish:
		c.pub <- msg.(*utp.Publish)
	case *utp.Pubnew:
		p := utp.Packet(&utp.Pubreceive{MessageID: m.MessageID})
		// persist outbound
		c.storeOutbound(p)
		c.send <- &PacketAndResult{p: p}
	case *utp.Pubreceipt:
		p := utp.Packet(&utp.Pubcomplete{MessageID: m.MessageID})
		// persist outbound
		c.storeOutbound(p)
		c.send <- &PacketAndResult{p: p}
	case *utp.Pubcomplete:
		mId := c.inboundID(m.MessageID)
		fmt.Println("conn::handler: pubcomp MessageID, InboundID ", m.MessageID, mId)
		r := c.getType(mId)
		if r != nil {
			r.flowComplete()
			c.freeID(mId)
		}
	}

	return nil
}

func (c *client) writeLoop(ctx context.Context) {
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
			case *utp.Disconnect:
				msg.r.(*DisconnectResult).flowComplete()
				mId := c.inboundID(m.MessageID)
				c.freeID(mId)
			}
			m, err := utp.Encode(msg.p)
			if err != nil {
				fmt.Println(err)
				return
			}
			c.conn.Write(m.Bytes())
		}
	}
}

func (c *client) dispatcher(ctx context.Context) {
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
			go func() {
				handler(c, m)
				m.Ack()
			}()
		}
	}
}

// keepalive - Send ping when connection unused for set period
// connection passed in to avoid race condition on shutdown
func (c *client) keepalive(ctx context.Context) {
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

			// if lastAction.After(live) && lastTouched.Before(timeout) {
			if lastAction.After(live) || lastTouched.After(live) {
				ping := &utp.Pingreq{}
				m, err := utp.Encode(ping)
				if err != nil {
					fmt.Println(err)
				}
				c.conn.Write(m.Bytes())
				c.updateLastTouched()
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
func ack(c *client, pkt *utp.Publish) func() {
	return func() {
		fmt.Println("conn::ack: pub, deliveryMode ", pkt.Info().DeliveryMode)
		switch pkt.Info().DeliveryMode {
		// DeliveryMode RELIABLE or BATCH
		case 1, 2:
			p := utp.Packet(&utp.Pubreceipt{MessageID: pkt.MessageID})
			// persist outbound
			c.storeOutbound(p)
			c.send <- &PacketAndResult{p: p}
		// DeliveryMode Express
		case 0:
			p := utp.Packet(&utp.Pubcomplete{MessageID: pkt.MessageID})
			// persist outbound
			c.storeOutbound(p)
			c.send <- &PacketAndResult{p: p}
		}
	}
}
