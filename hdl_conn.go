package unitdb

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
)

// readIdleTimeout is how long the client waits for any message from the
// server before it considers the connection dead.
var readIdleTimeout = 120 * time.Second

// Connect takes a connected net.Conn and performs the initial handshake. Paramaters are:
// conn - Connected net.Conn
// cm - Connect Message
func Connect(conn net.Conn, cm *utp.Connect) (rc uint8, epoch int32, cid int32, err error) {
	m, err := lp.Encode(cm)
	if err != nil {
		return utp.ErrRefusedServerUnavailable, 0, 0, err
	}
	if _, err := conn.Write(m.Bytes()); err != nil {
		return utp.ErrRefusedServerUnavailable, 0, 0, err
	}
	return verifyCONNACK(conn)
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func verifyCONNACK(conn net.Conn) (uint8, int32, int32, error) {
	ca, err := lp.Read(conn)
	if err != nil {
		return utp.ErrRefusedServerUnavailable, 0, 0, err
	}
	if ca == nil {
		return utp.ErrRefusedServerUnavailable, 0, 0, errors.New("nil connect acknowledge message")
	}

	pack, ok := ca.(*utp.ControlMessage)
	if !ok {
		return utp.ErrRefusedServerUnavailable, 0, 0, errors.New("first message must be connect acknowledge message")
	}

	connack := &utp.ConnectAcknowledge{}
	connack.FromBinary(utp.FixedHeader{MessageType: utp.CONNECT, FlowControl: utp.ACKNOWLEDGE}, pack.Message)

	return connack.ReturnCode, connack.Epoch, connack.ConnID, nil
}

// Handle handles incoming messages
func (c *client) readLoop(ctx context.Context) (err error) {
	var msg lp.MessagePack
	defer func() {
		c.internalConnLost(err)
	}()

	reader := bufio.NewReaderSize(c.conn, 65536)

	for {
		// Refresh the read deadline for every message so that only a
		// connection with no traffic at all, not even keepalive pings, is closed.
		c.conn.SetReadDeadline(time.Now().Add(c.readIdleTimeout))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeC:
			return nil
		default:
			// Unpack an incoming Message
			msg, err = lp.Read(reader)
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
func (c *client) handler(inMsg lp.MessagePack) error {
	c.updateLastAction()

	switch inMsg.Type() {
	case utp.FLOWCONTROL:
		ctrlMsg := *inMsg.(*utp.ControlMessage)
		switch ctrlMsg.FlowControl {
		case utp.ACKNOWLEDGE:
			switch ctrlMsg.MessageType {
			case utp.PINGREQ:
				c.updateLastTouched()
			case utp.SUBSCRIBE, utp.UNSUBSCRIBE, utp.RELAY, utp.PUBLISH:
				// Resumed messages have no result to complete.
				mId := c.inboundID(ctrlMsg.MessageID)
				if r := c.getType(mId); r != nil {
					r.flowComplete()
					c.freeID(mId)
				}
			}
		case utp.NOTIFY:
			recv := &utp.ControlMessage{
				MessageID:   ctrlMsg.MessageID,
				MessageType: utp.PUBLISH,
				FlowControl: utp.RECEIVE,
			}
			c.sendMessage(&MessageAndResult{m: recv})
		case utp.COMPLETE:
			mId := c.inboundID(ctrlMsg.MessageID)
			r := c.getType(mId)
			if r != nil {
				r.flowComplete()
				c.freeID(mId)
			}
		}
	case utp.PUBLISH:
		select {
		case c.pub <- inMsg.(*utp.Publish):
		case <-c.closeC:
		}
	case utp.DISCONNECT:
		go c.serverDisconnect(errors.New("server initiated disconnect")) // no harm in calling this if the connection is already down (better than stopping!)
	}

	return nil
}

func (c *client) writeLoop(ctx context.Context) (err error) {
	var buf bytes.Buffer

	defer func() { c.internalConnLost(err) }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeC:
			return
		case outMsg, ok := <-c.send:
			if !ok {
				// Channel closed.
				return
			}
			switch msg := outMsg.m.(type) {
			case *utp.Disconnect:
				outMsg.r.(*DisconnectResult).flowComplete()
				mId := c.inboundID(msg.MessageID)
				c.freeID(mId)
			}
			buf, err = lp.Encode(outMsg.m)
			if err != nil {
				return err
			}
			c.conn.Write(buf.Bytes())
		}
	}
}

func (c *client) dispatcher(ctx context.Context) (err error) {
	defer func() { c.internalConnLost(err) }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeC:
			return
		case pub, ok := <-c.pub:
			if !ok {
				// Channel closed.
				return
			}
			msg := messageFromPublish(pub, ack(c, pub))
			// dispatch message to default callback function
			go func() {
				c.notifier.notify(msg.messages)
				msg.Ack()
			}()
		}
	}
}

// keepalive sends a ping when nothing was received for the keep alive period,
// and closes the connection when a ping is not answered within the ping timeout.
func (c *client) keepalive(ctx context.Context) {
	keepAlive := time.Duration(c.opts.keepAlive) * time.Second
	interval := keepAlive / 2
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	pingTicker := time.NewTicker(interval)
	defer pingTicker.Stop()

	var pingSent time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case <-pingTicker.C:
			// lastTouched is when the server last answered a ping.
			if lastTouched := c.lastTouched.Load().(time.Time); pingSent.After(lastTouched) {
				if time.Since(pingSent) >= c.opts.pingTimeout {
					go c.internalConnLost(errors.New("pingresp not received, disconnecting"))
					return
				}
				continue
			}
			if time.Since(c.lastAction.Load().(time.Time)) >= keepAlive {
				pingSent = TimeNow()
				c.sendMessage(&MessageAndResult{m: &utp.Pingreq{}})
			}
		}
	}
}

// ack acknowledges a Message
func ack(c *client, pub *utp.Publish) func() {
	return func() {
		switch pub.Info().DeliveryMode {
		// DeliveryMode RELIABLE or BATCH
		case 1, 2:
			rec := &utp.ControlMessage{
				MessageID:   pub.MessageID,
				MessageType: utp.PUBLISH,
				FlowControl: utp.RECEIPT,
			}
			// persist outbound
			c.storeOutbound(rec)
			c.sendMessage(&MessageAndResult{m: rec})
		// DeliveryMode Express
		case 0:
			ack := &utp.ControlMessage{
				MessageID:   pub.MessageID,
				MessageType: utp.PUBLISH,
				FlowControl: utp.ACKNOWLEDGE,
			}
			// persist outbound
			c.storeOutbound(ack)
			c.sendMessage(&MessageAndResult{m: ack})
		}
	}
}
