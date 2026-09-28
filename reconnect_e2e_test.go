package unitdb

// Reconnection tests: a client with WithAutoReconnect loses its server and
// connects again by itself. They use the cluster harness, so that a server
// can be killed and started without disturbing the other tests.

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
)

// connEvents records a client's connection handler calls.
type connEvents struct {
	lost      chan error
	connected chan struct{}
}

func newConnEvents() *connEvents {
	return &connEvents{lost: make(chan error, 16), connected: make(chan struct{}, 16)}
}

func (e *connEvents) options() []Options {
	return []Options{
		WithConnectionLostHandler(func(_ Client, err error) { e.lost <- err }),
		WithConnectionHandler(func(Client) { e.connected <- struct{}{} }),
	}
}

func (e *connEvents) waitLost(t *testing.T) error {
	t.Helper()
	select {
	case err := <-e.lost:
		return err
	case <-time.After(e2eTimeout):
		t.Fatal("the connection lost handler was not called")
		return nil
	}
}

func (e *connEvents) waitConnected(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-e.connected:
	case <-time.After(timeout):
		t.Fatal("the client did not reconnect")
	}
}

// publishTo publishes payload on topic from a raw connection to n.
func publishTo(t *testing.T, n *clusterNode, clientID, topic, payload string) {
	t.Helper()
	pub := rawConnAt(t, n, clientID, true)
	pub.publish(1, topic, payload, 0)
}

func TestReconnectToAnotherNode(t *testing.T) {
	c := startCluster(t)
	dead, other := c.nodes[0], c.nodes[1]
	clientID := newClientIDAt(t, other.tcpAddr)
	events := newConnEvents()
	// NewClient adds its target after the AddServer servers: the client
	// connects to the node about to die first.
	opts := append(events.options(), AddServer("tcp://"+dead.tcpAddr), WithAutoReconnect(), WithMaxReconnectInterval(500*time.Millisecond))
	client := clusterClient(t, "tcp://"+other.tcpAddr, clientID, opts...)
	events.waitConnected(t, e2eTimeout) // the first connection

	got := collect(t, client, "rc.move...")
	ts := topics("rc.move")
	waitResult(t, "subscribe", client.SubscribeMultiple(ts))
	settle()
	for _, topic := range ts {
		publishTo(t, other, clientID, topic, "before>"+topic)
	}
	var want []string
	for _, topic := range ts {
		want = append(want, "before>"+topic)
	}
	expectPayloads(t, got, want...)

	dead.stop()
	if err := events.waitLost(t); err == nil {
		t.Error("the connection lost handler was called without an error")
	}
	events.waitConnected(t, e2eTimeout)
	// The survivors fail the dead node over, then the resubscribed topics
	// deliver again.
	c.kill(dead)
	want = want[:0]
	for _, topic := range ts {
		publishTo(t, other, clientID, topic, "after>"+topic)
		want = append(want, "after>"+topic)
	}
	expectPayloads(t, got, want...)
}

func TestReconnectAfterServerRestart(t *testing.T) {
	c := startCluster(t)
	n := c.nodes[0]
	clientID := newClientIDAt(t, n.tcpAddr)
	events := newConnEvents()
	client := clusterClient(t, "tcp://"+n.tcpAddr, clientID,
		append(events.options(), WithAutoReconnect(), WithMaxReconnectInterval(500*time.Millisecond))...)
	events.waitConnected(t, e2eTimeout)
	got := collect(t, client, "rc.restart")
	waitResult(t, "subscribe", client.Subscribe("rc.restart"))

	n.stop()
	events.waitLost(t)
	// The server is down for a while: the client keeps trying.
	time.Sleep(time.Second)
	if err := n.start(); err != nil {
		t.Fatalf("restart %s: %v", n.name, err)
	}
	events.waitConnected(t, e2eTimeout)
	settle()
	publishTo(t, n, clientID, "rc.restart", "back")
	expectPayloads(t, got, "back")
}

func TestReconnectQueuesCalls(t *testing.T) {
	c := startCluster(t)
	n, subNode := c.nodes[0], c.nodes[1]
	clientID := newClientIDAt(t, subNode.tcpAddr)
	sub := rawConnAt(t, subNode, clientID, true)
	sub.subscribe(1, "rc.queued", 0)
	settle()

	events := newConnEvents()
	client := clusterClient(t, "tcp://"+n.tcpAddr, clientID,
		append(events.options(), WithAutoReconnect(), WithMaxReconnectInterval(500*time.Millisecond), WithWriteTimeout(20*time.Second))...)
	events.waitConnected(t, e2eTimeout)

	n.stop()
	events.waitLost(t)
	// Published while the client is down: it waits for the connection.
	r := client.Publish("rc.queued", []byte("queued"))
	time.Sleep(500 * time.Millisecond)
	if err := n.start(); err != nil {
		t.Fatalf("restart %s: %v", n.name, err)
	}
	events.waitConnected(t, e2eTimeout)
	waitResult(t, "publish made while reconnecting", r)
	sub.waitFor("queued message", func(m lp.MessagePack) bool {
		pub, ok := m.(*utp.Publish)
		return ok && len(pub.Messages) > 0 && string(pub.Messages[0].Payload) == "queued"
	})
}

func TestDisconnectWhileReconnecting(t *testing.T) {
	c := startCluster(t)
	n := c.nodes[0]
	clientID := newClientIDAt(t, n.tcpAddr)
	events := newConnEvents()
	client := clusterClient(t, "tcp://"+n.tcpAddr, clientID,
		append(events.options(), WithAutoReconnect(), WithMaxReconnectInterval(200*time.Millisecond))...)
	events.waitConnected(t, e2eTimeout)

	n.stop()
	events.waitLost(t)
	start := time.Now()
	if err := client.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Disconnect while reconnecting took %v", d)
	}
	// Once disconnected, it does not come back.
	if err := n.start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events.connected:
		t.Fatal("the client reconnected after Disconnect")
	case <-time.After(2 * time.Second):
	}
	if r := client.Publish("rc.closed", []byte("x")); r != nil {
		if _, err := r.Get(context.Background(), time.Second); !errors.Is(err, errClientClosed) {
			t.Fatalf("Publish after Disconnect: %v, want %v", err, errClientClosed)
		}
	}
}

func TestNoReconnectByDefault(t *testing.T) {
	c := startCluster(t)
	n := c.nodes[0]
	clientID := newClientIDAt(t, n.tcpAddr)
	events := newConnEvents()
	client := clusterClient(t, "tcp://"+n.tcpAddr, clientID, events.options()...)
	events.waitConnected(t, e2eTimeout)

	n.stop()
	events.waitLost(t)
	if err := n.start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events.connected:
		t.Fatal("the client reconnected without WithAutoReconnect")
	case <-time.After(2 * time.Second):
	}
	r := client.Publish("rc.default", []byte("x"))
	if _, err := r.Get(context.Background(), time.Second); err == nil {
		t.Fatal("Publish on a client whose connection was lost succeeded")
	}
}

func TestReconnectWhenNodeDrains(t *testing.T) {
	c := startCluster(t)
	draining, other := c.nodes[0], c.nodes[1]
	clientID := newClientIDAt(t, other.tcpAddr)
	events := newConnEvents()
	// The draining node first: NewClient adds its target after AddServer.
	client := clusterClient(t, "tcp://"+other.tcpAddr, clientID,
		append(events.options(), AddServer("tcp://"+draining.tcpAddr), WithAutoReconnect(), WithMaxReconnectInterval(500*time.Millisecond))...)
	events.waitConnected(t, e2eTimeout)
	got := collect(t, client, "rc.drain...")
	ts := topics("rc.drain")
	waitResult(t, "subscribe", client.SubscribeMultiple(ts))
	settle()

	// A deploy stops the node gracefully: it leaves the cluster and closes
	// its clients' connections, and they move to another node.
	draining.cmd.Process.Signal(syscall.SIGTERM)
	events.waitLost(t)
	events.waitConnected(t, e2eTimeout)
	select {
	case <-draining.exited:
	case <-time.After(20 * time.Second):
		t.Fatal("the node did not exit on SIGTERM")
	}
	settle()
	var want []string
	for _, topic := range ts {
		publishTo(t, other, clientID, topic, "moved>"+topic)
		want = append(want, "moved>"+topic)
	}
	expectPayloads(t, got, want...)
}
