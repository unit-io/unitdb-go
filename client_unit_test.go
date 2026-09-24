package unitdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

func TestOptions(t *testing.T) {
	o := new(options)
	WithDefaultOptions().set(o)
	if o.keepAlive != 30 || o.connectTimeout != 30*time.Second || o.cleanSession {
		t.Fatalf("unexpected defaults %+v", o)
	}

	for _, opt := range []Options{
		WithClientID("cid"),
		WithSessionKey(7),
		WithInsecure(),
		WithUserNamePassword("user", []byte("pass")),
		WithCleanSession(),
		WithKeepAlive(10 * time.Second),
		WithPingTimeout(2 * time.Second),
		WithWriteTimeout(3 * time.Second),
		WithConnectTimeout(4 * time.Second),
		WithStorePath("/tmp/store"),
		WithStoreSize(1024),
		WithBatchDuration(time.Minute),
		WithBatchByteThreshold(100),
		WithBatchCountThreshold(5),
		WithResumeSubs(),
	} {
		opt.set(o)
	}
	if o.clientID != "cid" || o.sessionKey != 7 || !o.insecureFlag || o.username != "user" || string(o.password) != "pass" {
		t.Fatalf("unexpected identity options %+v", o)
	}
	if !o.cleanSession || o.keepAlive != 10 || o.pingTimeout != 2*time.Second || o.writeTimeout != 3*time.Second || o.connectTimeout != 4*time.Second {
		t.Fatalf("unexpected connection options %+v", o)
	}
	if o.storePath != "/tmp/store" || o.storeSize != 1024 || !o.resumeSubs {
		t.Fatalf("unexpected store options %+v", o)
	}
	// The batch duration is capped.
	if o.batchDuration != maxBatchDuration || o.batchByteThreshold != 100 || o.batchCountThreshold != 5 {
		t.Fatalf("unexpected batch options %+v", o)
	}

	// Thresholds above the maximum are ignored.
	WithBatchByteThreshold(maxPubBytes + 1).set(o)
	WithBatchCountThreshold(maxPubCount + 1).set(o)
	if o.batchByteThreshold != 100 || o.batchCountThreshold != 5 {
		t.Fatalf("thresholds above the maximum must be ignored: %+v", o)
	}

	// The store log release duration must exceed the write timeout.
	WithStoreLogReleaseDuration(time.Second).set(o)
	if o.storeLogReleaseDuration == time.Second {
		t.Fatal("release duration below the write timeout must be ignored")
	}
	WithStoreLogReleaseDuration(time.Minute).set(o)
	if o.storeLogReleaseDuration != time.Minute {
		t.Fatalf("release duration = %v, want 1m", o.storeLogReleaseDuration)
	}
}

func TestAddServer(t *testing.T) {
	tests := []struct {
		target     string
		scheme     string
		host       string
		useDefault bool // NewClient target (grpc default) instead of AddServer (tcp default)
	}{
		{":6080", "grpc", "127.0.0.1:6080", true},
		{"localhost:6080", "grpc", "localhost:6080", true},
		{"tcp://localhost:6060", "tcp", "localhost:6060", true},
		{":6060", "tcp", "127.0.0.1:6060", false},
		{"grpc://localhost:6080", "grpc", "localhost:6080", false},
	}
	for _, tt := range tests {
		o := new(options)
		if tt.useDefault {
			o.addServer(tt.target)
		} else {
			AddServer(tt.target).set(o)
		}
		if len(o.servers) != 1 {
			t.Fatalf("%q: %d servers", tt.target, len(o.servers))
		}
		if u := o.servers[0]; u.Scheme != tt.scheme || u.Host != tt.host {
			t.Errorf("%q: got %s://%s, want %s://%s", tt.target, u.Scheme, u.Host, tt.scheme, tt.host)
		}
	}
}

func TestPubSubRelOptions(t *testing.T) {
	po := new(pubOptions)
	for _, opt := range []PubOptions{WithPubDeliveryMode(2), WithPubDelay(1500 * time.Millisecond), WithTTL("1m")} {
		opt.set(po)
	}
	if po.deliveryMode != 2 || po.delay != 1500 || po.ttl != "1m" {
		t.Fatalf("unexpected publish options %+v", po)
	}

	so := new(subOptions)
	for _, opt := range []SubOptions{WithSubDeliveryMode(1), WithSubDelay(time.Second)} {
		opt.set(so)
	}
	if so.deliveryMode != 1 || so.delay != 1000 {
		t.Fatalf("unexpected subscribe options %+v", so)
	}

	ro := new(relOptions)
	WithLast("10m").set(ro)
	if ro.last != "10m" {
		t.Fatalf("unexpected relay options %+v", ro)
	}
}

func TestNewConnectMsgFromOptions(t *testing.T) {
	o := new(options)
	WithDefaultOptions().set(o)
	WithClientID("cid").set(o)
	WithInsecure().set(o)
	WithSessionKey(9).set(o)
	WithUserNamePassword("user", []byte("pass")).set(o)
	WithBatchDuration(250 * time.Millisecond).set(o)

	o.addServer("grpc://localhost:6080")
	m := newConnectMsgFromOptions(o, o.servers[0])
	if m.ClientID != "cid" || !m.InsecureFlag || m.SessKey != 9 || m.Username != "user" || string(m.Password) != "pass" {
		t.Fatalf("unexpected connect message %+v", m)
	}
	if m.KeepAlive != 30 || m.BatchDuration != 250 || m.BatchCountThreshold != maxPubCount {
		t.Fatalf("unexpected connect message %+v", m)
	}

	// Credentials in the url take precedence.
	o.addServer("grpc://urluser:urlpass@localhost:6080")
	m = newConnectMsgFromOptions(o, o.servers[1])
	if m.Username != "urluser" || string(m.Password) != "urlpass" {
		t.Fatalf("url credentials ignored: %+v", m)
	}
}

func TestMessageIds(t *testing.T) {
	ids := messageIds{index: make(map[MID]Result), resumedIds: make(map[MID]struct{})}
	ids.reset(100)
	r1, r2 := &PublishResult{}, &SubscribeResult{}
	a, b := ids.nextID(r1), ids.nextID(r2)
	if a != 99 || b != 98 {
		t.Fatalf("ids %d, %d; want 99, 98", a, b)
	}
	if ids.getType(a) != r1 || ids.getType(b) != r2 {
		t.Fatal("getType returned the wrong result")
	}
	ids.freeID(a)
	if ids.getType(a) != nil {
		t.Fatal("freed id must not have a result")
	}
}

func TestMessageIdsSkipsResumedID(t *testing.T) {
	ids := messageIds{index: make(map[MID]Result), resumedIds: make(map[MID]struct{})}
	ids.reset(100)
	ids.resumeID(99)
	done := make(chan MID, 1)
	go func() { done <- ids.nextID(&PublishResult{}) }()
	select {
	case id := <-done:
		if id == 99 {
			t.Fatal("nextID returned a resumed id")
		}
	case <-time.After(time.Second):
		t.Fatal("nextID deadlocked")
	}
}

func TestOutboundInboundID(t *testing.T) {
	c := &client{connID: 1000}
	for _, mid := range []MID{999, 1, 500} {
		if got := c.inboundID(c.outboundID(mid)); got != mid {
			t.Fatalf("inboundID(outboundID(%d)) = %d", mid, got)
		}
	}
}

func TestResult(t *testing.T) {
	ctx := context.Background()

	r := &result{complete: make(chan struct{})}
	if done, err := r.Get(ctx, 10*time.Millisecond); done || err != nil {
		t.Fatalf("incomplete result: done=%v err=%v", done, err)
	}

	r.flowComplete()
	r.flowComplete() // completing twice must not panic
	if done, err := r.Get(ctx, time.Second); !done || err != nil {
		t.Fatalf("completed result: done=%v err=%v", done, err)
	}

	r = &result{complete: make(chan struct{})}
	want := errors.New("boom")
	r.setError(want)
	if done, err := r.Get(ctx, time.Second); !done || err != want {
		t.Fatalf("errored result: done=%v err=%v", done, err)
	}

	r = &result{complete: make(chan struct{})}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if done, _ := r.Get(cctx, time.Second); !done {
		t.Fatal("Get must return when the context is done")
	}
}

func TestMessageFromPublish(t *testing.T) {
	acked := 0
	pub := &utp.Publish{MessageID: 3, DeliveryMode: 1, Messages: []*utp.PublishMessage{
		{Topic: "a", Payload: []byte("1")},
		{Topic: "b", Payload: []byte("2")},
	}}
	m := messageFromPublish(pub, func() { acked++ })
	if m.MessageID() != 3 || m.DeliveryMode() != 1 || len(m.Messages()) != 2 || m.Messages()[1].Topic != "b" {
		t.Fatalf("unexpected message %+v", m)
	}
	m.Ack()
	m.Ack()
	if acked != 1 {
		t.Fatalf("ack called %d times, want 1", acked)
	}
}

func TestNotifier(t *testing.T) {
	n := newNotifier(10)
	defer n.close()

	// Without observers notifications are dropped.
	n.notify([]*PubMessage{{Topic: "a"}})

	got := make(chan []*PubMessage, 1)
	n.addFilter(func(notice *Notice) error {
		got <- notice.messages
		return nil
	})
	n.notify([]*PubMessage{{Topic: "b", Payload: []byte("x")}})
	select {
	case msgs := <-got:
		if len(msgs) != 1 || msgs[0].Topic != "b" {
			t.Fatalf("unexpected notice %v", msgs)
		}
	case <-time.After(time.Second):
		t.Fatal("filter not called")
	}
}

func TestBatchManagerGroupsMessages(t *testing.T) {
	// The publish loops are not started; the queue is read directly.
	m := &batchManager{
		opts:         &batchOptions{batchDuration: time.Minute, batchCountThreshold: 2, batchByteThreshold: 1 << 20},
		batchGroup:   make(map[timeID]*batch),
		publishQueue: make(chan *batch, 1),
	}

	r1 := m.add(0, &utp.PublishMessage{Topic: "a", Payload: []byte("1")})
	r2 := m.add(0, &utp.PublishMessage{Topic: "a", Payload: []byte("2")})
	if r1 != r2 {
		t.Fatal("messages in the same batch window must share a result")
	}
	if r := m.add(int32(2*time.Minute/time.Millisecond), &utp.PublishMessage{Topic: "a", Payload: []byte("delayed")}); r == r1 {
		t.Fatal("a delayed message must go to a later batch")
	}

	// Going over the count threshold pushes the batch.
	m.add(0, &utp.PublishMessage{Topic: "a", Payload: []byte("3")})
	select {
	case b := <-m.publishQueue:
		if len(b.pubMessages) != 3 || b.r != r1 {
			t.Fatalf("pushed batch has %d messages", len(b.pubMessages))
		}
	default:
		t.Fatal("batch over the count threshold was not pushed")
	}
}

func TestBatchManagerByteThreshold(t *testing.T) {
	m := &batchManager{
		opts:         &batchOptions{batchDuration: time.Minute, batchCountThreshold: 100, batchByteThreshold: 4},
		batchGroup:   make(map[timeID]*batch),
		publishQueue: make(chan *batch, 1),
	}
	m.add(0, &utp.PublishMessage{Topic: "a", Payload: []byte("12345")})
	select {
	case b := <-m.publishQueue:
		if len(b.pubMessages) != 1 {
			t.Fatalf("pushed batch has %d messages", len(b.pubMessages))
		}
	default:
		t.Fatal("batch over the byte threshold was not pushed")
	}
}

func TestNotifierConcurrentNotify(t *testing.T) {
	n := newNotifier(10)
	defer n.close()

	const senders, perSender = 8, 100
	got := make(chan struct{}, senders*perSender)
	n.addFilter(func(*Notice) error {
		got <- struct{}{}
		return nil
	})

	for s := 0; s < senders; s++ {
		go func() {
			for i := 0; i < perSender; i++ {
				n.notify([]*PubMessage{{Topic: "t"}})
			}
		}()
		// Filters may be added while notifications are in flight.
		go n.addFilter(func(*Notice) error { return nil })
	}
	for i := 0; i < senders*perSender; i++ {
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatalf("received %d of %d notifications", i, senders*perSender)
		}
	}
}

func TestNotifierNotifyAfterClose(t *testing.T) {
	n := newNotifier(1)
	n.addFilter(func(*Notice) error { return nil })
	n.close()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			n.notify([]*PubMessage{{Topic: "t"}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notify blocked after close")
	}
}
