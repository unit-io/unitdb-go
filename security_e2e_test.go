package unitdb

// End to end tests of the server's v2 client ids and topic keys: renewed
// client ids, keygen with a ttl and uuids, revocation and vouching. They run
// against a server with them (unitdb's security stage 2), and are skipped
// with an older one.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb-go/internal/store"
	"github.com/unit-io/unitdb/server/utp"
)

// requireV2Server starts the shared server, and skips the test unless it
// issues v2 client ids and topic keys.
func requireV2Server(t *testing.T) {
	t.Helper()
	requireServer(t)
	if _, err := os.Stat(filepath.Join(serverSourceDir(), "server", "internal", "revocation.go")); err != nil || !refusesInsecure() {
		t.Skip("the server predates v2 client ids and topic keys")
	}
}

// startStandalone starts a server of its own, which a test may stop and
// start again, with allow_insecure and the extra config fields given.
func startStandalone(t *testing.T, extra string) *clusterNode {
	t.Helper()
	bin, _, _ := buildServer()
	tcpAddr, grpcAddr := freeAddr(), freeAddr()
	dbDir := t.TempDir()
	confName := fmt.Sprintf("standalone-%s.conf", strings.ReplaceAll(tcpAddr, ":", "-"))
	confPath := filepath.Join(filepath.Dir(bin), confName)
	conf := fmt.Sprintf(`{
		"listen": %q,
		"grpc_listen": %q,
		"logging_level": "Error",
		"encryption_config": {"key": %q, "identifier": "local"},
		"allow_insecure": true,
		%s
		"store_config": {"reset": true, "adapters": {"unitdb": {"mem_size": 16777216}}}
	}`, tcpAddr, grpcAddr, testKey, extra)
	if err := os.WriteFile(confPath, []byte(conf), 0644); err != nil {
		t.Fatal(err)
	}
	n := &clusterNode{name: "standalone", tcpAddr: tcpAddr, grpcAddr: grpcAddr, logs: &syncBuffer{},
		args: []string{bin, "-config", confName, "-db_path", filepath.Join(dbDir, "db")}}
	t.Cleanup(func() {
		n.stop()
		os.Remove(confPath)
	})
	if err := n.start(); err != nil {
		t.Fatalf("start the server: %v\n%s", err, n.logs.String())
	}
	return n
}

// connectCode connects to the server at addr with clientID, and returns the
// server's return code.
func connectCode(t *testing.T, addr, clientID string) uint8 {
	t.Helper()
	c := dialRawAt(t, addr)
	defer c.conn.Close()
	c.send(&utp.Connect{ClientID: clientID, KeepAlive: 30, SessKey: int32(nextSessKey())})
	m := c.waitFor("connect acknowledge", func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.MessageType == utp.CONNECT
	})
	ack := &utp.ConnectAcknowledge{}
	ack.FromBinary(utp.FixedHeader{}, m.(*utp.ControlMessage).Message)
	return ack.ReturnCode
}

// isV2ClientID reports whether id is shaped as a v2 client id.
func isV2ClientID(id string) bool { return len(id) == 94 && validClientID(id) }

// isUUID reports whether uuid is a uuid as the server gives it: decimal, not 0.
func isUUID(uuid string) bool {
	n, err := strconv.ParseUint(uuid, 10, 64)
	return err == nil && n != 0
}

// expectErrorStatus waits for an error notice with status on errs, the
// payloads of unitdb/error/.
func expectErrorStatus(t *testing.T, errs <-chan string, status int, what string) {
	t.Helper()
	timer := time.NewTimer(e2eTimeout)
	defer timer.Stop()
	for {
		select {
		case p := <-errs:
			var notice struct {
				Status int `json:"status"`
			}
			if json.Unmarshal([]byte(p), &notice) == nil && notice.Status == status {
				return
			}
		case <-timer.C:
			t.Fatalf("%s: no error notice with status %d", what, status)
		}
	}
}

// requestStatus waits for r, a request to the server's API, and returns the
// status of the server's refusal, or 200.
func requestStatus(t *testing.T, desc string, r Result) int {
	t.Helper()
	done, err := r.Get(t.Context(), e2eTimeout)
	if !done {
		t.Fatalf("%s: timed out", desc)
	}
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		return reqErr.Status
	}
	if err != nil {
		t.Fatalf("%s: %v", desc, err)
	}
	return r.(interface{ Status() int }).Status()
}

// TestE2EClientIDRenewal connects with a v1 client id, which the server
// renews: the client adopts the renewed id, tells the application, connects
// with it again after it loses its connection, and a client created with it
// later opens the same local store and resumes its session.
func TestE2EClientIDRenewal(t *testing.T) {
	requireV2Server(t)
	n := startStandalone(t, "")
	v1 := mintID(t, "-v1")
	if len(v1) != 52 {
		t.Fatalf("mintid -v1 gave %q", v1)
	}

	dir := storeDir(t)
	sessKey := nextSessKey()
	renewals := make(chan string, 4)
	events := newConnEvents()
	opts := append(events.options(),
		WithStorePath(dir),
		WithSessionKey(sessKey),
		WithAutoReconnect(),
		WithMaxReconnectInterval(500*time.Millisecond),
		WithClientIDHandler(func(_ Client, id string) { renewals <- id }),
	)
	c := newE2EClient(t, "tcp://"+n.tcpAddr, v1, opts...)
	events.waitConnected(t, e2eTimeout)

	var renewed string
	select {
	case renewed = <-renewals:
	case <-time.After(e2eTimeout):
		t.Fatal("the client id handler was not called")
	}
	if !isV2ClientID(renewed) {
		t.Fatalf("renewed client id %q is not a v2 one", renewed)
	}
	if got := c.ClientID(); got != renewed {
		t.Fatalf("ClientID() = %q, want the renewed %q", got, renewed)
	}
	// The store stays in the old id's directory, which the new id links to.
	if got := resolveStore(dir, renewed); got != filepath.Join(dir, v1) {
		t.Fatalf("the renewed id's store is %q, want %q", got, filepath.Join(dir, v1))
	}
	waitResult(t, "publish", c.Publish("e2e.renew", []byte("first")))

	// The client reconnects with the renewed id: the server, which renews v1
	// ids, sends no new one.
	n.stop()
	events.waitLost(t)
	if err := n.start(); err != nil {
		t.Fatalf("restart the server: %v\n%s", err, n.logs.String())
	}
	events.waitConnected(t, e2eTimeout)
	waitResult(t, "publish after the reconnect", c.Publish("e2e.renew", []byte("again")))
	select {
	case id := <-renewals:
		t.Fatalf("the client was renewed again, with %q: it reconnected with the v1 id", id)
	case <-time.After(time.Second):
	}
	if got := c.ClientID(); got != renewed {
		t.Fatalf("ClientID() = %q after the reconnect", got)
	}
	c.Disconnect()
	for i := 0; i < 200 && store.IsOpen(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	// The run left a publish the server never acknowledged, in its session.
	if err := store.Open(filepath.Join(dir, v1), 1<<27, false); err != nil {
		t.Fatal(err)
	}
	rawSess, err := store.Session.Get(uint64(sessKey))
	if err != nil || len(rawSess) < 4 {
		store.Close()
		t.Fatalf("the session of the first run is not stored: %v", err)
	}
	sessID := binary.LittleEndian.Uint32(rawSess)
	store.Log.PersistOutbound(sessID, &utp.Publish{MessageID: 9, Messages: []*utp.PublishMessage{{Topic: "e2e.renew", Payload: []byte("unfinished")}}})
	store.Close()

	// The next run, created with the renewed id the application kept,
	// resumes it.
	subscriber := connectRawAt(t, n.tcpAddr, renewed)
	subscriber.subscribe(1, "e2e.renew", 0)
	newE2EClient(t, "tcp://"+n.tcpAddr, renewed, WithStorePath(dir), WithSessionKey(sessKey))
	m := subscriber.waitFor("resumed publish", isPublishOn("e2e.renew"))
	if got := string(m.(*utp.Publish).Messages[0].Payload); got != "unfinished" {
		t.Fatalf("payload %q, want %q", got, "unfinished")
	}
	if fi, err := os.Stat(filepath.Join(dir, renewed)); err != nil || fi.IsDir() {
		t.Fatalf("the renewed id has a store of its own: %v", err)
	}
}

// TestE2EClientIDExpiry checks that an id near the end of its lifetime is
// renewed, and that an expired id is refused with return code 2.
func TestE2EClientIDExpiry(t *testing.T) {
	requireV2Server(t)
	// Renewed past 4 of its 6 seconds, expired after 1.
	ageing := mintID(t, "-ttl", "6s")
	expired := mintID(t, "-ttl", "1s")
	time.Sleep(4500 * time.Millisecond)

	c, err := NewClient(tcpTarget(), expired, WithStorePath(storeDir(t)), WithSessionKey(nextSessKey()))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Connect()
	var cerr *ConnectError
	if !errors.As(err, &cerr) || cerr.ReturnCode != ConnRefusedIDRejected {
		if err == nil {
			c.Disconnect()
		}
		t.Fatalf("Connect with an expired id: %v, want return code %d", err, ConnRefusedIDRejected)
	}

	renewals := make(chan string, 1)
	c = newE2EClientSecurity(t, tcpTarget(), ageing, false, WithClientIDHandler(func(_ Client, id string) { renewals <- id }))
	select {
	case id := <-renewals:
		if !isV2ClientID(id) || id == ageing || c.ClientID() != id {
			t.Fatalf("renewed with %q, ClientID() %q", id, c.ClientID())
		}
	case <-time.After(e2eTimeout):
		t.Fatal("an id past 80% of its lifetime was not renewed")
	}
}

// TestE2EKeygenAndRevoke checks keygen's uuids and ttl, and revocation by
// uuid of keys and client ids.
func TestE2EKeygenAndRevoke(t *testing.T) {
	requireV2Server(t)
	primary := newClientID(t)
	c := newE2EClientSecurity(t, tcpTarget(), primary, false)
	errs := collect(t, c, "unitdb/error/")

	r := c.Keygen(
		KeyRequest{Topic: "e2e.revoke", Type: "rw"},
		KeyRequest{Topic: "e2e.short", Type: "rw", TTL: 2 * time.Second},
		KeyRequest{Topic: "e2e.later", Type: "rw", TTL: time.Hour},
	)
	waitResult(t, "keygen", r)
	keys := r.(*KeygenResult).Keys()
	if len(keys) != 3 {
		t.Fatalf("keygen gave %d keys", len(keys))
	}
	for i, topic := range []string{"e2e.revoke", "e2e.short", "e2e.later"} {
		if k := keys[i]; k.Topic != topic || len(k.Key) != 48 || !isUUID(k.UUID) {
			t.Fatalf("key %d: %+v", i, k)
		}
	}
	revoked, short, later := keys[0], keys[1], keys[2]

	// The keys work, whatever '-' and '_' they hold.
	got := collect(t, c, "e2e.revoke")
	waitResult(t, "subscribe", c.Subscribe(revoked.Key+"/e2e.revoke"))
	waitResult(t, "publish", c.Publish(revoked.Key+"/e2e.revoke", []byte("before")))
	expectPayloads(t, got, "before")
	waitResult(t, "subscribe with the short lived key", c.Subscribe(short.Key+"/e2e.short"))

	// Revoked, a key is refused.
	if status := requestStatus(t, "revoke", c.Revoke(revoked.UUID, time.Time{})); status != 200 {
		t.Fatalf("revoke: status %d", status)
	}
	waitResult(t, "subscribe with a revoked key", c.Subscribe(revoked.Key+"/e2e.revoke"))
	expectErrorStatus(t, errs, 401, "a revoked key")

	// Until a time, and bad requests.
	if status := requestStatus(t, "revoke until", c.Revoke(later.UUID, time.Now().Add(time.Hour))); status != 200 {
		t.Fatalf("revoke until: status %d", status)
	}
	for _, uuid := range []string{"0", "not-a-uuid"} {
		if status := requestStatus(t, "revoke "+uuid, c.Revoke(uuid, time.Time{})); status != 400 {
			t.Fatalf("revoke %q: status %d, want 400", uuid, status)
		}
	}
	if status := requestStatus(t, "revoke until a past time", c.Revoke(later.UUID, time.Now().Add(-time.Hour))); status != 400 {
		t.Fatalf("revoke until a past time: status %d, want 400", status)
	}

	// Expired, a key is refused too.
	time.Sleep(2500 * time.Millisecond)
	waitResult(t, "subscribe with an expired key", c.Subscribe(short.Key+"/e2e.short"))
	expectErrorStatus(t, errs, 401, "an expired key")

	// A secondary client id, revoked by its uuid, is refused at connect.
	idr := c.RequestClientID()
	waitResult(t, "client id request", idr)
	secondary := idr.(*ClientIDResult)
	if !isV2ClientID(secondary.ClientID()) || !isUUID(secondary.UUID()) {
		t.Fatalf("client id %q, uuid %q", secondary.ClientID(), secondary.UUID())
	}
	if code := connectCode(t, e2e.tcpAddr, secondary.ClientID()); code != ConnAccepted {
		t.Fatalf("connect with the secondary id: return code %d", code)
	}
	if status := requestStatus(t, "revoke the secondary id", c.Revoke(secondary.UUID(), time.Time{})); status != 200 {
		t.Fatalf("revoke the secondary id: status %d", status)
	}
	if code := connectCode(t, e2e.tcpAddr, secondary.ClientID()); code != ConnRefusedIDRejected {
		t.Fatalf("connect with a revoked id: return code %d, want %d", code, ConnRefusedIDRejected)
	}
	// The connection stays, and the key revoked until later is refused.
	waitResult(t, "subscribe with a key revoked until later", c.Subscribe(later.Key+"/e2e.later"))
	expectErrorStatus(t, errs, 401, "a key revoked until later")
}

// TestE2ERevokeAll checks that revoking all revokes the contract's keys and
// client ids, the primary client's own included.
func TestE2ERevokeAll(t *testing.T) {
	requireV2Server(t)
	primary := newClientID(t)
	c := newE2EClientSecurity(t, tcpTarget(), primary, false)
	errs := collect(t, c, "unitdb/error/")

	r := c.Keygen(KeyRequest{Topic: "e2e.all", Type: "rw"})
	waitResult(t, "keygen", r)
	key := r.(*KeygenResult).Keys()[0].Key
	idr := c.RequestClientID()
	waitResult(t, "client id request", idr)
	secondary := idr.(*ClientIDResult).ClientID()
	// What was issued in the second of the revocation is not revoked.
	time.Sleep(1100 * time.Millisecond)

	if status := requestStatus(t, "revoke all", c.RevokeAll()); status != 200 {
		t.Fatalf("revoke all: status %d", status)
	}
	waitResult(t, "subscribe with a revoked key", c.Subscribe(key+"/e2e.all"))
	expectErrorStatus(t, errs, 401, "a key revoked with all")
	for _, id := range []string{secondary, primary} {
		if code := connectCode(t, e2e.tcpAddr, id); code != ConnRefusedIDRejected {
			t.Fatalf("connect with an id revoked with all: return code %d, want %d", code, ConnRefusedIDRejected)
		}
	}
}

// TestE2EVouch checks that a connection a service vouches for needs no
// topic keys, and stays a secondary client's.
func TestE2EVouch(t *testing.T) {
	requireV2Server(t)
	contract := 1 + rand.Intn(1<<30)
	service := mintID(t, "-service", "-contract", strconv.Itoa(contract))
	other := mintID(t, "-service", "-contract", strconv.Itoa(contract+1))
	serviceConn := connectRaw(t, service)
	user := serviceConn.secondaryClientID()

	c := newE2EClientSecurity(t, tcpTarget(), user, false)
	errs := collect(t, c, "unitdb/error/")
	got := collect(t, c, "e2e.vouch")

	// Without a key, the user's subscription is refused.
	waitResult(t, "subscribe without a key", c.Subscribe("e2e.vouch"))
	expectErrorStatus(t, errs, 400, "a subscription without a key")

	// A service of another contract can't vouch for it.
	if status := requestStatus(t, "vouch with another contract's service", c.Vouch(other)); status != 403 {
		t.Fatalf("vouch with another contract's service: status %d, want 403", status)
	}
	// Nor can a client id that is not a service's.
	if status := requestStatus(t, "vouch with the user's id", c.Vouch(user)); status != 403 {
		t.Fatalf("vouch with a user's id: status %d, want 403", status)
	}

	if status := requestStatus(t, "vouch", c.Vouch(service)); status != 200 {
		t.Fatalf("vouch: status %d", status)
	}
	waitResult(t, "subscribe once vouched for", c.Subscribe("e2e.vouch"))
	settle()
	serviceConn.publish(1, "e2e.vouch", "vouched", 0)
	expectPayloads(t, got, "vouched")

	// Vouched for, the user is no primary client.
	if status := requestStatus(t, "revoke", c.Revoke("1", time.Time{})); status != 403 {
		t.Fatalf("a vouched for client's revoke: status %d, want 403", status)
	}
}
