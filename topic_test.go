package unitdb

import (
	"strings"
	"testing"
	"time"
)

func parseTopic(t *testing.T, text string) *topic {
	t.Helper()
	tp := new(topic)
	if !tp.parse(text) {
		t.Fatalf("parse(%q) failed", text)
	}
	return tp
}

func TestTopicParse(t *testing.T) {
	tests := []struct {
		text      string
		wantTopic string
		wantParts []string
		wantOpts  map[string]string
	}{
		{"teams.alpha.ch1", "teams.alpha.ch1", []string{"teams", "alpha", "ch1"}, nil},
		{"teams.alpha?ttl=1m&delay=10", "teams.alpha", []string{"teams", "alpha"}, map[string]string{"ttl": "1m", "delay": "10"}},
		{"teams...", "teams", []string{"teams", "..."}, nil},
		{"teams.*.ch1", "teams.*.ch1", []string{"teams", "*", "ch1"}, nil},
		// Options without a value are ignored.
		{"teams?flag", "teams", []string{"teams"}, map[string]string{}},
	}
	for _, tt := range tests {
		tp := parseTopic(t, tt.text)
		if tp.topic != tt.wantTopic {
			t.Errorf("parse(%q).topic = %q, want %q", tt.text, tp.topic, tt.wantTopic)
		}
		if strings.Join(tp.parts, "|") != strings.Join(tt.wantParts, "|") {
			t.Errorf("parse(%q).parts = %v, want %v", tt.text, tp.parts, tt.wantParts)
		}
		for k, v := range tt.wantOpts {
			if got, ok := tp.getOption(k); !ok || got != v {
				t.Errorf("parse(%q).getOption(%q) = %q, %v; want %q", tt.text, k, got, ok, v)
			}
		}
	}
}

func TestTopicWire(t *testing.T) {
	tests := []struct{ text, want string }{
		{"teams.alpha", "teams.alpha"},
		{"KEY/teams.alpha", "KEY/teams.alpha"},
		{"KEY/teams.alpha?ttl=1m", "KEY/teams.alpha"},
		{"teams...", "teams..."},
		{"KEY/teams...?last=1m", "KEY/teams..."},
		{"unitdb/keygen", "unitdb/keygen"},
	}
	for _, tt := range tests {
		if got := parseTopic(t, tt.text).wire(); got != tt.want {
			t.Errorf("parse(%q).wire() = %q, want %q", tt.text, got, tt.want)
		}
	}

	// Matching ignores the key.
	if tp := parseTopic(t, "KEY/teams.alpha"); tp.topic != "teams.alpha" || tp.key != "KEY" {
		t.Fatalf("topic %q key %q", tp.topic, tp.key)
	}
}

func TestTopicParseInvalid(t *testing.T) {
	for _, text := range []string{"", "/", "?"} {
		if new(topic).parse(text) {
			t.Errorf("parse(%q) succeeded", text)
		}
	}
}

func TestTopicValidate(t *testing.T) {
	deep := strings.TrimSuffix(strings.Repeat("a.", TopicMaxDepth+1), ".")
	tests := []struct {
		text    string
		wantErr bool
	}{
		{"teams.alpha", false},
		{"teams...", false},
		{"teams.*.ch1", false},
		{"teams.a*.ch1", true}, // wildcard part longer than one character
		{"teams...ch1", true},  // multi wildcard not at the end
		{deep, true},
		{strings.Repeat("a", TopicMaxLength+1), true},
	}
	for _, tt := range tests {
		tp := new(topic)
		tp.parse(tt.text)
		err := tp.validate(validateMinLength, validateMaxLenth, validateMaxDepth, validateMultiWildcard, validateTopicParts)
		if (err != nil) != tt.wantErr {
			name := tt.text
			if len(name) > 20 {
				name = name[:20] + "..."
			}
			t.Errorf("validate(%q) error = %v, wantErr %v", name, err, tt.wantErr)
		}
	}

	if err := validateWildcards(parseTopic(t, "teams.*")); err == nil {
		t.Error("validateWildcards must reject wildcards")
	}
	if err := validateWildcards(parseTopic(t, "teams.alpha")); err != nil {
		t.Errorf("validateWildcards(teams.alpha) = %v", err)
	}
}

func TestTopicMatches(t *testing.T) {
	tests := []struct {
		sub, pub string
		want     bool
	}{
		{"teams.alpha", "teams.alpha", true},
		{"teams.alpha", "teams.beta", false},
		{"teams...", "teams.alpha", true},
		{"teams...", "teams.alpha.ch1", true},
		{"teams...", "other.alpha", false},
		{"teams.*", "teams.alpha", true},
		{"teams.*", "teams", false},
		{"teams.*", "teams.alpha.ch1", false},
		{"teams.*.ch1", "teams.alpha.ch1", true},
		{"teams.*.ch1", "teams.alpha.ch2", false},
		{"teams.alpha", "teams.alpha.ch1", false},
		{"teams.alpha.ch1", "teams.alpha", false},
		{"...", "anything.at.all", true},
	}
	for _, tt := range tests {
		sub := parseTopic(t, tt.sub)
		// The special "..." subscription is matched on the raw topic.
		if tt.sub == "..." {
			sub.topic = TopicMultiWildcardSymbol
		}
		if got := sub.matches(parseTopic(t, tt.pub)); got != tt.want {
			t.Errorf("%q matches %q = %v, want %v", tt.sub, tt.pub, got, tt.want)
		}
	}
}

func TestTopicFilter(t *testing.T) {
	sub := parseTopic(t, "groups.private...")
	f := &TopicFilter{subscriptionTopic: sub, updates: make(chan []*PubMessage, 1)}

	if err := f.filter(&Notice{messages: []*PubMessage{{Topic: "groups.public.x", Payload: []byte("no")}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case msgs := <-f.Updates():
		t.Fatalf("unexpected update %v", msgs)
	default:
	}

	if err := f.filter(&Notice{messages: []*PubMessage{{Topic: "groups.private.1.message", Payload: []byte("yes")}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case msgs := <-f.Updates():
		if len(msgs) != 1 || string(msgs[0].Payload) != "yes" {
			t.Fatalf("unexpected update %v", msgs)
		}
	case <-time.After(time.Second):
		t.Fatal("no update for a matching message")
	}
}

func TestTopicFilterBatch(t *testing.T) {
	sub := parseTopic(t, "groups.private...")
	f := &TopicFilter{subscriptionTopic: sub, updates: make(chan []*PubMessage, 1)}
	notice := &Notice{messages: []*PubMessage{
		{Topic: "groups.private.1", Payload: []byte("a")},
		{Topic: "groups.public.1", Payload: []byte("b")},
		{Topic: "groups.private.2", Payload: []byte("c")},
	}}
	if err := f.filter(notice); err != nil {
		t.Fatal(err)
	}
	msgs := <-f.Updates()
	if len(msgs) != 2 || string(msgs[0].Payload) != "a" || string(msgs[1].Payload) != "c" {
		var got []string
		for _, m := range msgs {
			got = append(got, string(m.Payload))
		}
		t.Fatalf("got payloads %v, want [a c]", got)
	}
}
