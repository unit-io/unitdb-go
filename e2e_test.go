package unitdb

// End to end tests: build the unitdb server from source, run it and drive it
// with the client. The server source is looked up in $UNITDB_SERVER_DIR, or
// in ../unitdb next to this repository. The tests are skipped with -short or
// when the server source is not found.
//
// The client store is process wide, so only one Client can be open at a time.
// Where a test needs a second party it uses rawConn, which speaks uTP directly.

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb-go/internal/store"
	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
)

const e2eTimeout = 10 * time.Second

var e2e struct {
	once     sync.Once
	skip     string // reason to skip, if the server can't be started
	err      error
	dir      string
	cmd      *exec.Cmd
	tcpAddr  string
	grpcAddr string
}

// lastSessKey hands out distinct session keys. The default session key is the
// client id epoch, which has second resolution, so clients created in the
// same second would share a server session.
var lastSessKey uint32 = 1 << 21

func nextSessKey() uint32 {
	return atomic.AddUint32(&lastSessKey, 1)
}

func TestMain(m *testing.M) {
	code := m.Run()
	stopServer()
	os.Exit(code)
}

// requireServer starts the shared server on first use.
func requireServer(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("end to end test skipped in short mode")
	}
	e2e.once.Do(startServer)
	if e2e.skip != "" {
		t.Skip(e2e.skip)
	}
	if e2e.err != nil {
		t.Fatal(e2e.err)
	}
}

func serverSourceDir() string {
	if dir := os.Getenv("UNITDB_SERVER_DIR"); dir != "" {
		return dir
	}
	return filepath.Join("..", "unitdb")
}

// build builds the server from source into e2e.dir, once.
var build struct {
	once sync.Once
	bin  string
	skip string
	err  error
}

func buildServer() (bin, skip string, err error) {
	build.once.Do(func() {
		src, err := filepath.Abs(serverSourceDir())
		if err == nil {
			_, err = os.Stat(filepath.Join(src, "server", "main.go"))
		}
		if err != nil {
			build.skip = fmt.Sprintf("unitdb server source not found in %s (set UNITDB_SERVER_DIR)", src)
			return
		}
		if e2e.dir, err = os.MkdirTemp("", "unitdb-e2e"); err != nil {
			build.err = err
			return
		}
		bin := filepath.Join(e2e.dir, "unitdb-server")
		cmd := exec.Command("go", "build", "-o", bin, "./server")
		cmd.Dir = src
		if out, err := cmd.CombinedOutput(); err != nil {
			build.err = fmt.Errorf("building the server: %v\n%s", err, out)
			return
		}
		build.bin = bin
	})
	return build.bin, build.skip, build.err
}

func startServer() {
	bin, skip, err := buildServer()
	if skip != "" || err != nil {
		e2e.skip, e2e.err = skip, err
		return
	}

	e2e.tcpAddr = freeAddr()
	e2e.grpcAddr = freeAddr()
	// The server reads the config next to its binary.
	conf := fmt.Sprintf(`{
		"listen": %q,
		"grpc_listen": %q,
		"logging_level": "Error",
		"encryption_config": {"key": "test-only-key-do-not-use-0000000", "identifier": "local"},
		"store_config": {"reset": true, "adapters": {"unitdb": {"mem_size": 16777216}}}
	}`, e2e.tcpAddr, e2e.grpcAddr)
	if err := os.WriteFile(filepath.Join(e2e.dir, "unitdb.conf"), []byte(conf), 0644); err != nil {
		e2e.err = err
		return
	}

	logFile, err := os.Create(filepath.Join(e2e.dir, "server.log"))
	if err != nil {
		e2e.err = err
		return
	}
	e2e.cmd = exec.Command(bin, "-db_path", filepath.Join(e2e.dir, "db"))
	e2e.cmd.Stdout = logFile
	e2e.cmd.Stderr = logFile
	if err := e2e.cmd.Start(); err != nil {
		e2e.err = err
		return
	}

	deadline := time.Now().Add(e2eTimeout)
	for _, addr := range []string{e2e.tcpAddr, e2e.grpcAddr} {
		for {
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				log, _ := os.ReadFile(logFile.Name())
				e2e.err = fmt.Errorf("server did not listen on %s: %v\n%s", addr, err, log)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func stopServer() {
	if e2e.cmd != nil && e2e.cmd.Process != nil {
		e2e.cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			e2e.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			e2e.cmd.Process.Kill()
		}
	}
	if e2e.dir != "" {
		os.RemoveAll(e2e.dir)
	}
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// rawConn is a minimal uTP connection used as the second party in a test.
type rawConn struct {
	t       *testing.T
	conn    net.Conn
	in      chan lp.MessagePack
	pending []lp.MessagePack
}

func dialRaw(t *testing.T) *rawConn {
	t.Helper()
	return dialRawAt(t, e2e.tcpAddr)
}

// dialRawAt opens a raw connection to the server at addr.
func dialRawAt(t *testing.T, addr string) *rawConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, e2eTimeout)
	if err != nil {
		t.Fatal(err)
	}
	c := &rawConn{t: t, conn: conn, in: make(chan lp.MessagePack, 64)}
	go func() {
		defer close(c.in)
		r := bufio.NewReader(conn)
		for {
			m, err := lp.Read(r)
			if err != nil {
				return
			}
			c.in <- m
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return c
}

func (c *rawConn) send(m lp.MessagePack) {
	c.t.Helper()
	buf, err := lp.Encode(m)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.t.Fatal(err)
	}
}

func (c *rawConn) waitFor(desc string, match func(lp.MessagePack) bool) lp.MessagePack {
	c.t.Helper()
	for i, m := range c.pending {
		if match(m) {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return m
		}
	}
	timer := time.NewTimer(e2eTimeout)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-c.in:
			if !ok {
				c.t.Fatalf("connection closed while waiting for %s", desc)
			}
			if match(m) {
				return m
			}
			c.pending = append(c.pending, m)
		case <-timer.C:
			c.t.Fatalf("timed out waiting for %s", desc)
		}
	}
}

func isPublishOn(topic string) func(lp.MessagePack) bool {
	return func(m lp.MessagePack) bool {
		pub, ok := m.(*utp.Publish)
		return ok && len(pub.Messages) > 0 && pub.Messages[0].Topic == topic
	}
}

func isControl(msgType utp.MessageType, flow utp.FlowControl, id uint16) func(lp.MessagePack) bool {
	return func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.MessageType == msgType && ctrl.FlowControl == flow && ctrl.MessageID == id
	}
}

// newClientID asks the server for a new primary client id.
func newClientID(t *testing.T) string {
	t.Helper()
	return newClientIDAt(t, e2e.tcpAddr)
}

// newClientIDAt asks the server at addr for a new primary client id.
func newClientIDAt(t *testing.T, addr string) string {
	t.Helper()
	c := dialRawAt(t, addr)
	// The keep alive makes the message long enough for the server's protocol sniffing.
	c.send(&utp.Connect{KeepAlive: 30})
	m := c.waitFor("assigned client id", isPublishOn("unitdb/clientid/"))
	return string(m.(*utp.Publish).Messages[0].Payload)
}

// connectRaw connects a rawConn with clientID in insecure mode.
func connectRaw(t *testing.T, clientID string) *rawConn {
	t.Helper()
	return connectRawAt(t, e2e.tcpAddr, clientID)
}

// connectRawAt connects a rawConn to the server at addr with clientID in
// insecure mode.
func connectRawAt(t *testing.T, addr, clientID string) *rawConn {
	t.Helper()
	c := dialRawAt(t, addr)
	c.send(&utp.Connect{ClientID: clientID, InsecureFlag: true, KeepAlive: 30, SessKey: int32(nextSessKey())})
	m := c.waitFor("connect acknowledge", func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.MessageType == utp.CONNECT
	})
	ack := &utp.ConnectAcknowledge{}
	ack.FromBinary(utp.FixedHeader{}, m.(*utp.ControlMessage).Message)
	if ack.ReturnCode != utp.Accepted {
		t.Fatalf("connect return code %d", ack.ReturnCode)
	}
	return c
}

// secondaryClientID requests a client id of the same contract as c.
func (c *rawConn) secondaryClientID() string {
	c.t.Helper()
	c.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/clientid"}}})
	m := c.waitFor("client id response", isPublishOn("unitdb/clientid"))
	var resp struct {
		Status int    `json:"status"`
		Key    string `json:"key"`
	}
	if err := json.Unmarshal(m.(*utp.Publish).Messages[0].Payload, &resp); err != nil || resp.Status != 200 {
		c.t.Fatalf("client id response %s: %v", m.(*utp.Publish).Messages[0].Payload, err)
	}
	return resp.Key
}

func (c *rawConn) publish(id uint16, topic, payload string, deliveryMode uint8) {
	c.t.Helper()
	c.send(&utp.Publish{MessageID: id, DeliveryMode: deliveryMode, Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte(payload)}}})
	c.waitFor("publish acknowledge", isControl(utp.PUBLISH, utp.ACKNOWLEDGE, id))
}

func (c *rawConn) subscribe(id uint16, topic string, deliveryMode uint8) {
	c.t.Helper()
	c.send(&utp.Subscribe{MessageID: id, Subscriptions: []*utp.Subscription{{Topic: topic, DeliveryMode: deliveryMode}}})
	c.waitFor("subscribe acknowledge", isControl(utp.SUBSCRIBE, utp.ACKNOWLEDGE, id))
}

// storeDir returns a client store directory that is removed once the store
// is closed; a client closes its store asynchronously when its connection is lost.
func storeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "unitdb-client")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for i := 0; i < 200 && store.IsOpen(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		os.RemoveAll(dir)
	})
	return dir
}

// newE2EClient creates and connects a client to target with a store in a temp dir.
func newE2EClient(t *testing.T, target, clientID string, opts ...Options) Client {
	t.Helper()
	return newE2EClientSecurity(t, target, clientID, true, opts...)
}

// newE2EClientSecurity is newE2EClient with the choice of insecure mode.
func newE2EClientSecurity(t *testing.T, target, clientID string, insecure bool, opts ...Options) Client {
	t.Helper()
	if insecure {
		opts = append([]Options{WithInsecure()}, opts...)
	}
	opts = append([]Options{
		WithStorePath(storeDir(t)),
		WithSessionKey(nextSessKey()),
		WithConnectionLostHandler(func(_ Client, err error) {
			t.Logf("connection lost: %v", err)
		}),
	}, opts...)
	c, err := NewClient(target, clientID, opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	// The context given to ConnectContext bounds the whole connection, not
	// only the connect, so Connect is used here.
	if err := c.Connect(); err != nil {
		// A failed Connect leaves the store open.
		store.Close()
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.Disconnect() })
	return c
}

func tcpTarget() string  { return "tcp://" + e2e.tcpAddr }
func grpcTarget() string { return "grpc://" + e2e.grpcAddr }

func waitResult(t *testing.T, desc string, r Result) {
	t.Helper()
	done, err := r.Get(context.Background(), e2eTimeout)
	if err != nil {
		t.Fatalf("%s: %v", desc, err)
	}
	if !done {
		t.Fatalf("%s: timed out", desc)
	}
}

// collect drains a topic filter into a channel of payloads until the test ends.
func collect(t *testing.T, c Client, topic string) <-chan string {
	t.Helper()
	f, err := c.TopicFilter(topic)
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan string, 1024)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case msgs := <-f.Updates():
				for _, m := range msgs {
					select {
					case out <- string(m.Payload):
					default:
					}
				}
			}
		}
	}()
	return out
}

// expectPayloads waits until every payload in want has been received at least once.
func expectPayloads(t *testing.T, got <-chan string, want ...string) {
	t.Helper()
	missing := make(map[string]bool)
	for _, w := range want {
		missing[w] = true
	}
	timer := time.NewTimer(e2eTimeout)
	defer timer.Stop()
	for len(missing) > 0 {
		select {
		case p := <-got:
			delete(missing, p)
		case <-timer.C:
			t.Fatalf("messages not received: %v", missing)
		}
	}
}

func expectNoPayload(t *testing.T, got <-chan string, d time.Duration) {
	t.Helper()
	select {
	case p := <-got:
		t.Fatalf("unexpected message %q", p)
	case <-time.After(d):
	}
}

func testPubSub(t *testing.T, target func() string) {
	requireServer(t)
	c := newE2EClient(t, target(), newClientID(t))
	got := collect(t, c, "e2e.room...")

	waitResult(t, "subscribe", c.Subscribe("e2e.room.lobby"))
	for i := 0; i < 3; i++ {
		waitResult(t, "publish", c.Publish("e2e.room.lobby", []byte(fmt.Sprintf("hello-%d", i))))
	}
	expectPayloads(t, got, "hello-0", "hello-1", "hello-2")

	waitResult(t, "unsubscribe", c.Unsubscribe("e2e.room.lobby"))
	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
}

func TestE2EPubSubTCP(t *testing.T) {
	testPubSub(t, tcpTarget)
}

func TestE2EPubSubGRPC(t *testing.T) {
	testPubSub(t, grpcTarget)
}

func TestE2EReceiveFromAnotherConnection(t *testing.T) {
	requireServer(t)
	publisher := connectRaw(t, newClientID(t))
	c := newE2EClient(t, grpcTarget(), publisher.secondaryClientID())
	got := collect(t, c, "e2e.chat")

	waitResult(t, "subscribe", c.Subscribe("e2e.chat"))
	publisher.publish(1, "e2e.chat", "from another connection", 0)
	expectPayloads(t, got, "from another connection")
}

func TestE2EPublishToAnotherConnection(t *testing.T) {
	requireServer(t)
	subscriber := connectRaw(t, newClientID(t))
	subscriber.subscribe(1, "e2e.orders", 0)

	c := newE2EClient(t, tcpTarget(), subscriber.secondaryClientID())
	waitResult(t, "publish", c.Publish("e2e.orders", []byte("order-1"), WithTTL("1m")))

	m := subscriber.waitFor("published message", isPublishOn("e2e.orders"))
	if got := string(m.(*utp.Publish).Messages[0].Payload); got != "order-1" {
		t.Fatalf("payload %q, want %q", got, "order-1")
	}
}

func TestE2EContractIsolation(t *testing.T) {
	requireServer(t)
	// A connection of another contract publishes on the same topic.
	stranger := connectRaw(t, newClientID(t))

	c := newE2EClient(t, tcpTarget(), newClientID(t))
	got := collect(t, c, "e2e.shared")
	waitResult(t, "subscribe", c.Subscribe("e2e.shared"))

	stranger.publish(1, "e2e.shared", "not for you", 0)
	expectNoPayload(t, got, 500*time.Millisecond)
}

func TestE2EReliableDelivery(t *testing.T) {
	requireServer(t)
	publisher := connectRaw(t, newClientID(t))
	c := newE2EClient(t, tcpTarget(), publisher.secondaryClientID())
	got := collect(t, c, "e2e.reliable")

	// The client answers the server's NOTIFY with RECEIVE, then acknowledges
	// the delivered message with RECEIPT.
	waitResult(t, "subscribe", c.Subscribe("e2e.reliable", WithSubDeliveryMode(1)))
	publisher.publish(1, "e2e.reliable", "reliable-1", 1)
	expectPayloads(t, got, "reliable-1")
}

func TestE2EBatchPublish(t *testing.T) {
	requireServer(t)
	c := newE2EClient(t, tcpTarget(), newClientID(t), WithBatchDuration(100*time.Millisecond))
	got := collect(t, c, "e2e.batch")
	waitResult(t, "subscribe", c.Subscribe("e2e.batch"))

	var results []Result
	for i := 0; i < 5; i++ {
		results = append(results, c.Publish("e2e.batch", []byte(fmt.Sprintf("batch-%d", i)), WithPubDeliveryMode(2)))
	}
	for _, r := range results {
		waitResult(t, "batch publish", r)
	}
	expectPayloads(t, got, "batch-0", "batch-1", "batch-2", "batch-3", "batch-4")
}

func TestE2ERelay(t *testing.T) {
	requireServer(t)
	publisher := connectRaw(t, newClientID(t))
	for i := 0; i < 3; i++ {
		publisher.publish(uint16(i+1), "e2e.history", fmt.Sprintf("stored-%d", i), 0)
	}

	// A client that was offline catches up on the stored messages.
	c := newE2EClient(t, grpcTarget(), publisher.secondaryClientID())
	got := collect(t, c, "e2e.history")
	waitResult(t, "relay", c.Relay([]string{"e2e.history"}, WithLast("1m")))
	expectPayloads(t, got, "stored-0", "stored-1", "stored-2")
	// Each stored message is delivered once.
	expectNoPayload(t, got, 500*time.Millisecond)
}

func TestE2EKeepAlive(t *testing.T) {
	requireServer(t)
	lost := make(chan error, 1)
	c := newE2EClient(t, tcpTarget(), newClientID(t),
		WithKeepAlive(2*time.Second),
		// The ping timeout is measured from the last ping, which on an idle
		// connection can be keep alive plus one ping interval ago, so a
		// shorter ping timeout drops healthy idle connections.
		WithPingTimeout(3*time.Second),
		WithConnectionLostHandler(func(_ Client, err error) { lost <- err }),
	)

	// Stay idle long enough for the client to ping the server.
	select {
	case err := <-lost:
		t.Fatalf("connection lost while idle: %v", err)
	case <-time.After(5 * time.Second):
	}
	waitResult(t, "publish after idle", c.Publish("e2e.keepalive", []byte("still here")))
}

func TestE2EConnectToClosedPort(t *testing.T) {
	requireServer(t)
	c, err := NewClient("tcp://"+freeAddr(), "", WithStorePath(storeDir(t)), WithConnectTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := c.Connect(); err == nil {
		t.Fatal("Connect to a closed port succeeded")
	}
}

func TestE2ERetryAfterFailedConnect(t *testing.T) {
	requireServer(t)
	c, err := NewClient("tcp://"+freeAddr(), "", WithStorePath(storeDir(t)), WithConnectTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(); err == nil {
		t.Fatal("Connect to a closed port succeeded")
	}
	newE2EClient(t, tcpTarget(), newClientID(t))
}

func TestE2EConnectWithUnknownClientID(t *testing.T) {
	requireServer(t)
	// Well formed, but not issued by this server.
	c, err := NewClient(tcpTarget(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", WithStorePath(storeDir(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Connect()
	if err == nil {
		c.Disconnect()
		t.Fatal("Connect with an unknown client id succeeded")
	}
	if !strings.Contains(err.Error(), "return code 2") {
		t.Fatalf("Connect error %q does not carry the return code", err)
	}
	// The failed client released the store.
	newE2EClient(t, tcpTarget(), newClientID(t))
}

func TestE2EFailoverToNextServer(t *testing.T) {
	requireServer(t)
	c := newE2EClient(t, "tcp://"+freeAddr(), newClientID(t), AddServer(tcpTarget()), WithConnectTimeout(time.Second))
	waitResult(t, "publish", c.Publish("e2e.failover", []byte("via the second server")))
}

func TestE2EConnectionOutlivesConnectTimeout(t *testing.T) {
	requireServer(t)
	for _, target := range []string{tcpTarget(), grpcTarget()} {
		t.Run(target[:strings.Index(target, ":")], func(t *testing.T) {
			c := newE2EClient(t, target, newClientID(t), WithConnectTimeout(time.Second))
			time.Sleep(1500 * time.Millisecond)
			waitResult(t, "publish after the connect timeout", c.Publish("e2e.timeout", []byte("still here")))
			c.Disconnect()
		})
	}
}

func TestE2EConnectContextBoundsConnection(t *testing.T) {
	requireServer(t)
	lost := make(chan error, 1)
	c, err := NewClient(tcpTarget(), newClientID(t),
		WithInsecure(),
		WithStorePath(storeDir(t)),
		WithSessionKey(nextSessKey()),
		WithConnectionLostHandler(func(_ Client, err error) {
			select {
			case lost <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.ConnectContext(ctx); err != nil {
		store.Close()
		t.Fatal(err)
	}
	defer c.Disconnect()

	waitResult(t, "publish", c.Publish("e2e.ctx", []byte("connected")))
	cancel()
	select {
	case err := <-lost:
		if err != context.Canceled {
			t.Fatalf("connection lost with error %v, want %v", err, context.Canceled)
		}
	case <-time.After(e2eTimeout):
		t.Fatal("connection outlived its context")
	}
}

func TestE2ESecurePubSub(t *testing.T) {
	requireServer(t)
	for _, target := range []func() string{tcpTarget, grpcTarget} {
		c := newE2EClientSecurity(t, target(), newClientID(t), false)

		// Ask the server for a key to the topic.
		responses := collect(t, c, "unitdb/keygen")
		waitResult(t, "keygen", c.Publish("unitdb/keygen", []byte(`[{"topic":"e2e.secure","type":"rw"}]`)))
		var resp []struct {
			Status int    `json:"status"`
			Key    string `json:"key"`
		}
		select {
		case p := <-responses:
			if err := json.Unmarshal([]byte(p), &resp); err != nil || len(resp) != 1 || resp[0].Status != 200 {
				t.Fatalf("keygen response %s: %v", p, err)
			}
		case <-time.After(e2eTimeout):
			t.Fatal("no keygen response")
		}
		key := resp[0].Key

		got := collect(t, c, "e2e.secure")
		waitResult(t, "subscribe", c.Subscribe(key+"/e2e.secure"))
		waitResult(t, "publish", c.Publish(key+"/e2e.secure", []byte("secured")))
		expectPayloads(t, got, "secured")
		c.Disconnect()
	}
}

func TestE2EWildcardSubscription(t *testing.T) {
	requireServer(t)
	publisher := connectRaw(t, newClientID(t))
	c := newE2EClient(t, tcpTarget(), publisher.secondaryClientID())
	got := collect(t, c, "e2e.sensors...")

	waitResult(t, "subscribe", c.Subscribe("e2e.sensors..."))
	publisher.publish(1, "e2e.sensors.room1.temp", "21", 0)
	expectPayloads(t, got, "21")
}

func TestE2EDisconnectWithoutConnect(t *testing.T) {
	requireServer(t)
	c, err := NewClient(tcpTarget(), "", WithStorePath(storeDir(t)), WithWriteTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("Disconnect took %v", d)
	}
	// The store is released for the next client.
	newE2EClient(t, tcpTarget(), newClientID(t))
}

func TestE2EReadDeadlineIsRefreshed(t *testing.T) {
	requireServer(t)
	old := readIdleTimeout
	readIdleTimeout = 4 * time.Second
	defer func() { readIdleTimeout = old }()

	lost := make(chan error, 1)
	c := newE2EClient(t, tcpTarget(), newClientID(t),
		WithKeepAlive(2*time.Second),
		WithPingTimeout(3*time.Second),
		WithConnectionLostHandler(func(_ Client, err error) { lost <- err }),
	)

	// Keepalive traffic must keep the connection open past the read timeout.
	select {
	case err := <-lost:
		t.Fatalf("connection lost: %v", err)
	case <-time.After(7 * time.Second):
	}
	waitResult(t, "publish", c.Publish("e2e.deadline", []byte("still here")))
}

func TestE2EDisconnectDuringTraffic(t *testing.T) {
	requireServer(t)
	for i := 0; i < 5; i++ {
		publisher := connectRaw(t, newClientID(t))
		c := newE2EClient(t, tcpTarget(), publisher.secondaryClientID())
		collect(t, c, "e2e.flood")
		waitResult(t, "subscribe", c.Subscribe("e2e.flood"))

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for id := uint16(1); ; id++ {
				select {
				case <-stop:
					return
				default:
				}
				publisher.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{{Topic: "e2e.flood", Payload: []byte("in")}}})
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Once the client is closed this must fail, not panic.
				c.Publish("e2e.flood", []byte("out"))
				c.Subscribe("e2e.flood")
			}
		}()

		time.Sleep(50 * time.Millisecond)
		c.Disconnect()
		time.Sleep(20 * time.Millisecond)
		close(stop)
		wg.Wait()

		if r := c.Publish("e2e.flood", []byte("after")); r != nil {
			if _, err := r.Get(context.Background(), time.Second); err == nil {
				t.Fatal("Publish on a disconnected client succeeded")
			}
		}
	}
}

func TestE2EResumeUnacknowledgedPublish(t *testing.T) {
	requireServer(t)
	subscriber := connectRaw(t, newClientID(t))
	subscriber.subscribe(1, "e2e.resume", 0)
	clientID := subscriber.secondaryClientID()

	// A previous run of the client left a publish the server never acknowledged.
	dir := storeDir(t)
	sessKey := nextSessKey()
	const sessID = 4242
	if err := store.Open(dir+"/"+clientID, 1<<27, false); err != nil {
		t.Fatal(err)
	}
	rawSess := []byte{0, 0, 0, 0}
	binary.LittleEndian.PutUint32(rawSess, sessID)
	store.Session.Put(uint64(sessKey), rawSess)
	store.Log.PersistOutbound(sessID, &utp.Publish{MessageID: 9, Messages: []*utp.PublishMessage{{Topic: "e2e.resume", Payload: []byte("unfinished")}}})
	store.Close()

	c := newE2EClient(t, tcpTarget(), clientID, WithStorePath(dir), WithSessionKey(sessKey))
	m := subscriber.waitFor("resumed publish", isPublishOn("e2e.resume"))
	if got := string(m.(*utp.Publish).Messages[0].Payload); got != "unfinished" {
		t.Fatalf("payload %q, want %q", got, "unfinished")
	}
	// The server's acknowledgement of the resumed message is handled.
	waitResult(t, "publish after resume", c.Publish("e2e.resume", []byte("next")))
}

func TestE2EShortPingTimeout(t *testing.T) {
	requireServer(t)
	lost := make(chan error, 1)
	c := newE2EClient(t, tcpTarget(), newClientID(t),
		WithKeepAlive(2*time.Second),
		// Shorter than the keep alive: answered pings must still keep an idle
		// connection open.
		WithPingTimeout(500*time.Millisecond),
		WithConnectionLostHandler(func(_ Client, err error) { lost <- err }),
	)
	select {
	case err := <-lost:
		t.Fatalf("connection lost while idle: %v", err)
	case <-time.After(5 * time.Second):
	}
	waitResult(t, "publish after idle", c.Publish("e2e.keepalive", []byte("still here")))
}

func TestE2EResumeNotificationTheServerForgot(t *testing.T) {
	requireServer(t)
	clientID := newClientID(t)

	// A previous run stored a notification the server no longer has.
	dir := storeDir(t)
	sessKey := nextSessKey()
	const sessID = 4343
	if err := store.Open(dir+"/"+clientID, 1<<27, false); err != nil {
		t.Fatal(err)
	}
	rawSess := []byte{0, 0, 0, 0}
	binary.LittleEndian.PutUint32(rawSess, sessID)
	store.Session.Put(uint64(sessKey), rawSess)
	store.Log.PersistInbound(sessID, &utp.ControlMessage{MessageID: 55, MessageType: utp.PUBLISH, FlowControl: utp.NOTIFY})
	store.Close()

	lost := make(chan error, 1)
	c := newE2EClient(t, tcpTarget(), clientID, WithStorePath(dir), WithSessionKey(sessKey),
		WithConnectionLostHandler(func(_ Client, err error) { lost <- err }))

	// The server completes the flow, the client forgets it and stays connected.
	deadline := time.Now().Add(e2eTimeout)
	for len(store.Log.Keys(sessID)) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("stale notification still stored")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-lost:
		t.Fatalf("connection lost: %v", err)
	default:
	}
	waitResult(t, "publish", c.Publish("e2e.stale", []byte("still connected")))
}

func TestE2ELargeMessage(t *testing.T) {
	requireServer(t)
	// A message body just under the frame limit: with its headers the gRPC
	// message is larger than gRPC's default 4 MiB receive limit.
	size := func(n int) int {
		return proto.Size(&pbx.Publish{MessageID: 65535, Messages: []*pbx.PublishMessage{{Topic: "e2e.large", Payload: make([]byte, n)}}})
	}
	n := 4<<20 - 100
	n += (lp.MaxFrameSize - 8) - size(n)
	payload := strings.Repeat("x", n)
	for _, target := range []func() string{tcpTarget, grpcTarget} {
		c := newE2EClient(t, target(), newClientID(t))
		got := collect(t, c, "e2e.large")
		waitResult(t, "subscribe", c.Subscribe("e2e.large"))
		waitResult(t, "publish", c.Publish("e2e.large", []byte(payload)))
		select {
		case p := <-got:
			if len(p) != len(payload) {
				t.Fatalf("%s: payload of %d bytes, want %d", target(), len(p), len(payload))
			}
		case <-time.After(e2eTimeout):
			t.Fatalf("%s: large message not delivered", target())
		}
		c.Disconnect()
	}
}

func TestE2EGRPCConnectionsAreReleased(t *testing.T) {
	requireServer(t)
	cycle := func() {
		c, err := NewClient(grpcTarget(), newClientID(t), WithInsecure(), WithStorePath(storeDir(t)), WithSessionKey(nextSessKey()))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Connect(); err != nil {
			store.Close()
			t.Fatal(err)
		}
		waitResult(t, "publish", c.Publish("e2e.release", []byte("x")))
		if err := c.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}
	// grpcGoroutines counts the goroutines running grpc code, waiting for
	// closing ones to exit.
	grpcGoroutines := func() int {
		count := func() int {
			buf := make([]byte, 4<<20)
			n := 0
			for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
				if strings.Contains(g, "google.golang.org/grpc") {
					n++
				}
			}
			return n
		}
		n := count()
		for i := 0; i < 50 && n > 0; i++ {
			time.Sleep(20 * time.Millisecond)
			n = count()
		}
		return n
	}

	// settled returns the goroutine count once closing goroutines have exited.
	settled := func() int {
		n := runtime.NumGoroutine()
		for i := 0; i < 50; i++ {
			time.Sleep(20 * time.Millisecond)
			m := runtime.NumGoroutine()
			if m == n {
				break
			}
			n = m
		}
		return n
	}

	cycle() // warm up shared state
	before := settled()
	const cycles = 10
	for i := 0; i < cycles; i++ {
		cycle()
	}
	// Each leaked grpc client connection leaves several goroutines behind.
	if n := grpcGoroutines(); n > 0 {
		t.Fatalf("%d grpc goroutines left after %d connect/disconnect cycles", n, cycles)
	}
	// Nothing else leaks either, such as the store's buffer pool.
	if after := settled(); after-before >= cycles {
		t.Fatalf("goroutines grew from %d to %d over %d connect/disconnect cycles", before, after, cycles)
	}
}
