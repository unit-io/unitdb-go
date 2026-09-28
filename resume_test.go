package unitdb

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/unit-io/unitdb-go/internal/store"
	"github.com/unit-io/unitdb/server/utp"
)

func openTestStore(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "unitdb-client-store")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Open(dir, 1<<20, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		os.RemoveAll(dir)
	})
}

func publishMsg(id uint16, payload string) *utp.Publish {
	return &utp.Publish{MessageID: id, DeliveryMode: 1, Messages: []*utp.PublishMessage{{Topic: "t", Payload: []byte(payload)}}}
}

func TestResume(t *testing.T) {
	openTestStore(t)
	const sess = 7

	// The server notified message 5, and the client's own message 5 and
	// message 16 (bit 4 set) wait for an acknowledgement.
	store.Log.PersistInbound(sess, &utp.ControlMessage{MessageID: 5, MessageType: utp.PUBLISH, FlowControl: utp.NOTIFY})
	store.Log.PersistOutbound(sess, publishMsg(5, "mine"))
	store.Log.PersistOutbound(sess, publishMsg(16, "bit4"))
	// Another session's message.
	store.Log.PersistOutbound(sess+1, publishMsg(6, "other session"))

	c := &client{
		send:       make(chan *MessageAndResult, 10),
		closeC:     make(chan struct{}),
		messageIds: messageIds{index: make(map[MID]Result), resumedIds: make(map[MID]struct{})},
	}
	done := make(chan struct{})
	go func() {
		c.resume(sess, false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resume blocked")
	}

	var receives, publishes []uint16
	for len(c.send) > 0 {
		switch m := (<-c.send).m.(type) {
		case *utp.ControlMessage:
			if m.FlowControl == utp.RECEIVE {
				receives = append(receives, m.MessageID)
			}
		case *utp.Publish:
			if string(m.Messages[0].Payload) == "other session" {
				t.Fatal("resumed another session's message")
			}
			publishes = append(publishes, m.MessageID)
		}
	}
	if len(receives) != 1 || receives[0] != 5 {
		t.Fatalf("receives %v, want [5]", receives)
	}
	if len(publishes) != 2 {
		t.Fatalf("resent publishes %v, want 5 and 16", publishes)
	}
}

func TestLogFlowCompletion(t *testing.T) {
	openTestStore(t)
	const sess = 9
	count := func() int { return len(store.Log.Keys(sess)) }

	// The client's message 3 and the server's message 3 share an id.
	store.Log.PersistOutbound(sess, publishMsg(3, "mine"))
	store.Log.PersistInbound(sess, &utp.ControlMessage{MessageID: 3, MessageType: utp.PUBLISH, FlowControl: utp.NOTIFY})
	if count() != 2 {
		t.Fatalf("%d log entries, want 2", count())
	}

	// The server acknowledges the client's message: only it is removed.
	store.Log.PersistInbound(sess, &utp.ControlMessage{MessageID: 3, MessageType: utp.PUBLISH, FlowControl: utp.ACKNOWLEDGE})
	keys := store.Log.Keys(sess)
	if len(keys) != 1 || !store.IsInboundKey(keys[0]) {
		t.Fatalf("after acknowledge: keys %v", keys)
	}

	// Receipt then complete finishes the server's message.
	store.Log.PersistOutbound(sess, &utp.ControlMessage{MessageID: 3, MessageType: utp.PUBLISH, FlowControl: utp.RECEIPT})
	if m, ok := store.Log.Get(keys[0]).(*utp.ControlMessage); !ok || m.FlowControl != utp.RECEIPT {
		t.Fatalf("receipt did not replace the notify: %v", m)
	}
	store.Log.PersistInbound(sess, &utp.ControlMessage{MessageID: 3, MessageType: utp.PUBLISH, FlowControl: utp.COMPLETE})
	if count() != 0 {
		t.Fatalf("%d log entries after complete, want 0", count())
	}

	// Express messages are not logged.
	store.Log.PersistInbound(sess, &utp.Publish{MessageID: 4})
	if count() != 0 {
		t.Fatal("express message logged")
	}
}

// TestBatchManagerConcurrentAdd adds messages from several goroutines while
// the publish loop flushes batches; run it with -race.
func TestBatchManagerConcurrentAdd(t *testing.T) {
	openTestStore(t)
	c := &client{
		opts:       new(options),
		send:       make(chan *MessageAndResult, 1024),
		closeC:     make(chan struct{}),
		connID:     1 << 20,
		messageIds: messageIds{index: make(map[MID]Result), resumedIds: make(map[MID]struct{})},
	}
	WithDefaultOptions().set(c.opts)
	c.newBatchManager(&batchOptions{batchDuration: 10 * time.Millisecond, batchCountThreshold: 5, batchByteThreshold: 1 << 20})

	const writers, perWriter = 4, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				c.batchManager.add(0, &utp.PublishMessage{Topic: "t", Payload: []byte("m")})
			}
		}()
	}
	wg.Wait()

	got := 0
	deadline := time.After(10 * time.Second)
	for got < writers*perWriter {
		select {
		case m := <-c.send:
			got += len(m.m.(*utp.Publish).Messages)
		case <-deadline:
			t.Fatalf("received %d of %d batched messages", got, writers*perWriter)
		}
	}
	c.batchManager.close()
}
