package unitdb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
)

// Connect takes a connected net.Conn and performs the initial handshake. Paramaters are:
// conn - Connected net.Conn
// cm - Connect Message
func Connect(conn net.Conn, cm *utp.Connect) (rc uint8, epoch int32, cid int32, err error) {
	m, err := lp.Encode(cm)
	if err != nil {
		fmt.Println(err)
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
func (c *client) readLoop(ctx context.Context) error {
	defer func() {
		log.Println("conn.readLoop: closing...")
		// c.closeW.Done()
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
			// Unpack an incoming Message
			msg, err := lp.Read(reader)
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
				mId := c.inboundID(ctrlMsg.MessageID)
				c.getType(mId).flowComplete()
				c.freeID(mId)
			}
		case utp.NOTIFY:
			recv := &utp.ControlMessage{
				MessageID:   ctrlMsg.MessageID,
				MessageType: utp.PUBLISH,
				FlowControl: utp.RECEIVE,
			}
			c.send <- &MessageAndResult{m: recv}
		case utp.COMPLETE:
			mId := c.inboundID(ctrlMsg.MessageID)
			r := c.getType(mId)
			if r != nil {
				r.flowComplete()
				c.freeID(mId)
			}
		}
	case utp.PUBLISH:
		c.pub <- inMsg.(*utp.Publish)
	case utp.DISCONNECT:
		go c.serverDisconnect(errors.New("server initiated disconnect")) // no harm in calling this if the connection is already down (better than stopping!)
	}

	return nil
}

func (c *client) writeLoop(ctx context.Context) {
	// defer c.closeW.Done()
	for {
		select {
		case <-ctx.Done():
			return
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
			buf, err := lp.Encode(outMsg.m)
			if err != nil {
				fmt.Println(err)
				// return
			}
			c.conn.Write(buf.Bytes())
		}
	}
}

func (c *client) dispatcher(ctx context.Context) {
	// defer c.closeW.Done()
	for {
		select {
		case <-ctx.Done():
			return
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

// keepalive - Send ping when connection unused for set period
// connection passed in to avoid race condition on shutdown
func (c *client) keepalive(ctx context.Context) {
	var pingInterval int32
	var pingSent time.Time

	if c.opts.keepAlive > 10 {
		pingInterval = 5
	} else {
		pingInterval = c.opts.keepAlive / 2
	}

	pingTicker := time.NewTicker(time.Duration(pingInterval * int32(time.Second)))
	defer func() {
		pingTicker.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case <-pingTicker.C:
			lastAction := c.lastAction.Load().(time.Time)
			lastTouched := c.lastTouched.Load().(time.Time)
			live := TimeNow().Add(-time.Duration(c.opts.keepAlive * int32(time.Second)))
			timeout := TimeNow().Add(-c.opts.pingTimeout)

			if lastAction.After(live) || lastTouched.After(live) {
				ping := &utp.Pingreq{}
				m, err := lp.Encode(ping)
				if err != nil {
					fmt.Println(err)
				}
				c.conn.Write(m.Bytes())
				c.updateLastTouched()
				pingSent = TimeNow()
			}
			if lastTouched.Before(timeout) && pingSent.Before(timeout) {
				go c.internalConnLost(errors.New("pingresp not received, disconnecting")) // no harm in calling this if the connection is already down (better than stopping!)
				return
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
			c.send <- &MessageAndResult{m: rec}
		// DeliveryMode Express
		case 0:
			ack := &utp.ControlMessage{
				MessageID:   pub.MessageID,
				MessageType: utp.PUBLISH,
				FlowControl: utp.ACKNOWLEDGE,
			}
			// persist outbound
			c.storeOutbound(ack)
			c.send <- &MessageAndResult{m: ack}
		}
	}
}
