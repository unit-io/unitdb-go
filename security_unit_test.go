package unitdb

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

// v2Key is shaped as a v2 topic key: 48 characters of base64url, with the
// '-' and '_' that v1 keys never hold, first and last too.
const v2Key = "-Ab_9xYz-_0123456789abcdefghijklmnopqrstuvwxyz_-"

// v2ClientID is shaped as a v2 client id: 94 characters of base64url.
var v2ClientID = "_" + strings.Repeat("aB-_", 23) + "-"

func TestV2KeyTopicParse(t *testing.T) {
	if len(v2Key) != 48 || len(v2ClientID) != 94 {
		t.Fatalf("sample lengths %d, %d", len(v2Key), len(v2ClientID))
	}
	tests := []struct {
		text, topic, wire string
		parts             []string
		opts              map[string]string
	}{
		{v2Key + "/teams.alpha", "teams.alpha", v2Key + "/teams.alpha", []string{"teams", "alpha"}, nil},
		{v2Key + "/teams.alpha?ttl=1m&last=2h", "teams.alpha", v2Key + "/teams.alpha", []string{"teams", "alpha"}, map[string]string{"ttl": "1m", "last": "2h"}},
		{v2Key + "/teams...", "teams", v2Key + "/teams...", []string{"teams", "..."}, nil},
		{v2Key + "/teams.*.ch1", "teams.*.ch1", v2Key + "/teams.*.ch1", []string{"teams", "*", "ch1"}, nil},
		{v2Key + "/...", "", v2Key + "/...", []string{"..."}, nil},
		{"-_/a-b_c.d", "a-b_c.d", "-_/a-b_c.d", []string{"a-b_c", "d"}, nil},
	}
	for _, tt := range tests {
		tp := parseTopic(t, tt.text)
		if tp.key != strings.SplitN(tt.text, "/", 2)[0] {
			t.Errorf("parse(%q).key = %q", tt.text, tp.key)
		}
		if tp.topic != tt.topic {
			t.Errorf("parse(%q).topic = %q, want %q", tt.text, tp.topic, tt.topic)
		}
		if got := tp.wire(); got != tt.wire {
			t.Errorf("parse(%q).wire() = %q, want %q", tt.text, got, tt.wire)
		}
		if strings.Join(tp.parts, "|") != strings.Join(tt.parts, "|") {
			t.Errorf("parse(%q).parts = %v, want %v", tt.text, tp.parts, tt.parts)
		}
		for k, v := range tt.opts {
			if got, ok := tp.getOption(k); !ok || got != v {
				t.Errorf("parse(%q).getOption(%q) = %q, %v", tt.text, k, got, ok)
			}
		}
		if err := tp.validate(validateMinLength, validateMaxLenth, validateMaxDepth, validateMultiWildcard, validateTopicParts); err != nil && tt.topic != "" {
			t.Errorf("validate(%q): %v", tt.text, err)
		}
	}

	// A subscription with a v2 key matches messages with or without one.
	sub := parseTopic(t, v2Key+"/teams.*")
	for _, pub := range []string{"teams.alpha", v2Key + "/teams.alpha"} {
		if !sub.matches(parseTopic(t, pub)) {
			t.Errorf("%q does not match %q", sub.wire(), pub)
		}
	}

	// Subscriptions with v2 keys are tracked, and forgotten, by their topic.
	c := &client{}
	c.trackSubscriptions([]*utp.Subscription{{Topic: parseTopic(t, v2Key+"/teams.alpha?last=1m").wire()}})
	if _, ok := c.subs[v2Key+"/teams.alpha"]; !ok {
		t.Fatalf("subscriptions %v", c.subs)
	}
	c.untrackSubscriptions([]string{v2Key + "/teams.alpha?last=1m"})
	if len(c.subs) != 0 {
		t.Fatalf("subscriptions left %v", c.subs)
	}
}

func TestValidClientID(t *testing.T) {
	for _, id := range []string{v2ClientID, strings.Repeat("A2", 26), "abc="} {
		if !validClientID(id) {
			t.Errorf("validClientID(%q) = false", id)
		}
	}
	for _, id := range []string{"", "a/b", "..", "a b", `a\b`, strings.Repeat("a", 256)} {
		if validClientID(id) {
			t.Errorf("validClientID(%q) = true", id)
		}
	}
}

func TestStoreLink(t *testing.T) {
	root := t.TempDir()
	const v1 = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMM"
	dir := resolveStore(root, v1)
	if dir != filepath.Join(root, v1) {
		t.Fatalf("store of %q in %q", v1, dir)
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		t.Fatal(err)
	}
	if got := resolveStore(root, ""); got != root {
		t.Fatalf("store of no id in %q", got)
	}

	// A renewed id finds the store of the id it renewed, through any number
	// of renewals.
	renewed := v2ClientID
	if err := linkStore(root, renewed, dir); err != nil {
		t.Fatal(err)
	}
	if got := resolveStore(root, renewed); got != dir {
		t.Fatalf("store of the renewed id in %q, want %q", got, dir)
	}
	again := "x" + v2ClientID[1:]
	if err := linkStore(root, again, resolveStore(root, renewed)); err != nil {
		t.Fatal(err)
	}
	if got := resolveStore(root, again); got != dir {
		t.Fatalf("store of the id renewed twice in %q, want %q", got, dir)
	}
	// The old id still opens its own store.
	if got := resolveStore(root, v1); got != dir {
		t.Fatalf("store of the old id in %q", got)
	}

	// An id with a store of its own keeps it.
	other := filepath.Join(root, "other")
	if err := os.MkdirAll(other, 0750); err != nil {
		t.Fatal(err)
	}
	if err := linkStore(root, "other", dir); err != nil {
		t.Fatal(err)
	}
	if got := resolveStore(root, "other"); got != other {
		t.Fatalf("store of an id with its own store in %q", got)
	}

	// A file that is not a link, or links out of the root, is not followed.
	for name, content := range map[string]string{
		"notalink": "some file",
		"escape":   storeLinkPrefix + "../elsewhere",
		"dotdot":   storeLinkPrefix + "..",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0640); err != nil {
			t.Fatal(err)
		}
		if got := resolveStore(root, name); got != filepath.Join(root, name) {
			t.Errorf("%s: store in %q", name, got)
		}
	}
	// No temporary files are left.
	matches, _ := filepath.Glob(filepath.Join(root, ".link-*"))
	if len(matches) != 0 {
		t.Fatalf("temporary files left: %v", matches)
	}
}

func TestRenewClientID(t *testing.T) {
	root := t.TempDir()
	const old = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMM"
	got := make(chan string, 2)
	o := new(options)
	WithDefaultOptions().set(o)
	WithStorePath(root).set(o)
	WithClientIDHandler(func(_ Client, id string) { got <- id }).set(o)
	o.setClientID(old)
	c := &client{opts: o, storeDir: filepath.Join(root, old)}

	// Neither an invalid id nor the same one is taken.
	c.serverMessages(1, &utp.Publish{Messages: []*utp.PublishMessage{{Topic: topicNewClientID, Payload: []byte("../evil")}}})
	c.serverMessages(1, &utp.Publish{Messages: []*utp.PublishMessage{{Topic: topicNewClientID, Payload: []byte(old)}}})
	if c.ClientID() != old {
		t.Fatalf("client id %q", c.ClientID())
	}

	c.serverMessages(1, &utp.Publish{Messages: []*utp.PublishMessage{{Topic: topicNewClientID, Payload: []byte(v2ClientID)}}})
	if c.ClientID() != v2ClientID {
		t.Fatalf("client id %q, want the renewed one", c.ClientID())
	}
	select {
	case id := <-got:
		if id != v2ClientID {
			t.Fatalf("handler called with %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
	select {
	case id := <-got:
		t.Fatalf("handler called again with %q", id)
	case <-time.After(50 * time.Millisecond):
	}
	if dir := resolveStore(root, v2ClientID); dir != filepath.Join(root, old) {
		t.Fatalf("store of the renewed id in %q", dir)
	}
	// The connect message carries the renewed id.
	if cm := newConnectMsgFromOptions(c.opts, &url.URL{}); cm.ClientID != v2ClientID {
		t.Fatalf("connect with %q", cm.ClientID)
	}
}

func TestRequestAnswers(t *testing.T) {
	c := &client{}
	send := func(topic string, r requestResult, gen uint64) *pendingRequest {
		req := &pendingRequest{topic: topic, r: r}
		c.pending = append(c.pending, req)
		if !c.writingRequest(req, gen) {
			t.Fatal("request not written")
		}
		return req
	}
	answer := func(gen uint64, topic, payload string) {
		c.serverMessages(gen, &utp.Publish{Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte(payload)}}})
	}
	get := func(r Result) error {
		done, err := r.Get(context.Background(), time.Second)
		if !done {
			t.Fatal("result not complete")
		}
		return err
	}

	k1 := &KeygenResult{RequestResult: newRequestResult()}
	k2 := &KeygenResult{RequestResult: newRequestResult()}
	rv := newRequestResult()
	id := &ClientIDResult{RequestResult: newRequestResult()}
	send(topicKeygen, k1, 1)
	send(topicRevoke, rv, 1)
	send(topicKeygen, k2, 1)
	send(topicClientID, id, 1)

	// Answers come in order per topic.
	answer(1, topicKeygen, `[{"status":200,"key":"`+v2Key+`","topic":"a.b","uuid":"123"}]`)
	answer(1, topicRevoke, `{"status":403,"message":"no"}`)
	answer(1, topicKeygen, `{"status":403,"message":"Unacceptable identifier"}`)
	answer(1, topicClientID, `{"status":200,"key":"`+v2ClientID+`","uuid":"77"}`)

	if err := get(k1); err != nil {
		t.Fatal(err)
	}
	if keys := k1.Keys(); len(keys) != 1 || keys[0] != (TopicKey{Topic: "a.b", Key: v2Key, UUID: "123"}) || k1.Status() != 200 {
		t.Fatalf("keys %+v, status %d", keys, k1.Status())
	}
	var reqErr *RequestError
	if err := get(rv); !errors.As(err, &reqErr) || reqErr.Status != 403 || reqErr.Message != "no" || rv.Status() != 403 {
		t.Fatalf("revoke: %v", err)
	}
	if err := get(k2); !errors.As(err, &reqErr) || reqErr.Status != 403 || len(k2.Keys()) != 0 {
		t.Fatalf("refused keygen: %v, keys %v", err, k2.Keys())
	}
	if err := get(id); err != nil || id.ClientID() != v2ClientID || id.UUID() != "77" {
		t.Fatalf("client id %q uuid %q: %v", id.ClientID(), id.UUID(), err)
	}

	// The requests of a lost connection fail, and their late answers are
	// not taken for the next connection's.
	lost := newRequestResult()
	queued := newRequestResult()
	send(topicService, lost, 2)
	queuedReq := &pendingRequest{topic: topicService, r: queued}
	c.pending = append(c.pending, queuedReq) // not written yet
	c.failRequests(2, errRequestConnLost)
	if err := get(lost); err != errRequestConnLost {
		t.Fatalf("lost request: %v", err)
	}
	answer(2, topicService, `{"status":200}`)
	if !c.writingRequest(queuedReq, 3) {
		t.Fatal("a queued request is not written on the next connection")
	}
	answer(3, topicService, `{"status":200}`)
	if err := get(queued); err != nil {
		t.Fatal(err)
	}

	// A request that failed before it was written is not written.
	closed := &pendingRequest{topic: topicRevoke, r: newRequestResult()}
	c.pending = append(c.pending, closed)
	c.failRequests(0, errClientClosed)
	if c.writingRequest(closed, 4) {
		t.Fatal("a failed request is written")
	}
	if len(c.pending) != 0 {
		t.Fatalf("pending requests left: %d", len(c.pending))
	}
}

func TestConnectError(t *testing.T) {
	var err error = &ConnectError{Server: "host:1", ReturnCode: ConnRefusedNotAuthorized}
	if !strings.Contains(err.Error(), "return code 4 (not authorized)") {
		t.Fatalf("error %q", err)
	}
	if ConnRefusedNotAuthorized != utp.ErrRefusedServerUnavailable || ConnRefusedIDRejected != utp.ErrRefusedIDRejected || ConnAccepted != utp.Accepted {
		t.Fatal("return codes differ from utp's")
	}
	if ReturnCodeText(0x42) != "unknown return code" {
		t.Fatal("unknown code named")
	}
}
