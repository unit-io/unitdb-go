package unitdb

// Cluster end to end tests: run a 3-node unitdb cluster, each node its own
// server process, and drive it with the client. They use the server built by
// the end to end tests and are skipped in the same cases.
//
// The cluster routes by topic: every contract and topic has an owner node,
// which holds the topic's subscriptions and stores its messages; the other
// nodes forward requests to it. The client can't compute owners (the ring is
// internal to the server), so each check uses many topics: with
// clusterTopics topics, every node owns some of them with overwhelming
// probability, and each topic must work whichever node owns it.

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	lp "github.com/unit-io/unitdb-go/internal/net"
	"github.com/unit-io/unitdb/server/utp"
)

// clusterTopics is the number of topics a check uses. The chance that one of
// three nodes owns none of them is 3*(2/3)^30, about 1 in 60,000.
const clusterTopics = 30

var clusterNames = []string{"one", "two", "three"}

type clusterNode struct {
	name     string
	rpcAddr  string
	tcpAddr  string
	grpcAddr string
	args     []string
	logs     *syncBuffer
	cmd      *exec.Cmd
	exited   chan struct{}
}

type testCluster struct {
	t     *testing.T
	nodes []*clusterNode
}

// syncBuffer collects a node's output.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startCluster starts a 3-node cluster with failover and waits for a leader.
func startCluster(t *testing.T) *testCluster {
	t.Helper()
	if testing.Short() {
		t.Skip("cluster test skipped in short mode")
	}
	bin, skip, err := buildServer()
	if skip != "" {
		t.Skip(skip)
	}
	if err != nil {
		t.Fatal(err)
	}

	type nodeConf struct {
		Name string `json:"name"`
		Addr string `json:"addr"`
	}
	c := &testCluster{t: t}
	var confNodes []nodeConf
	for _, name := range clusterNames {
		n := &clusterNode{name: name, rpcAddr: freeAddr(), tcpAddr: freeAddr(), grpcAddr: freeAddr(), logs: &syncBuffer{}}
		c.nodes = append(c.nodes, n)
		confNodes = append(confNodes, nodeConf{name, n.rpcAddr})
	}
	clusterConf, _ := json.Marshal(map[string]interface{}{
		"self":  "", // set per node with -cluster_self
		"nodes": confNodes,
		"failover": map[string]interface{}{
			"enabled":         true,
			"heartbeat":       100,
			"vote_after":      8,
			"node_fail_after": 16,
		},
	})

	for _, n := range c.nodes {
		dbDir, err := os.MkdirTemp("", "unitdb-cluster-"+n.name)
		if err != nil {
			t.Fatal(err)
		}
		// The server reads its config next to its binary.
		confName := fmt.Sprintf("cluster-%s-%s.conf", n.name, strings.ReplaceAll(n.tcpAddr, ":", "-"))
		conf := fmt.Sprintf(`{
			"listen": %q,
			"grpc_listen": %q,
			"logging_level": "Error",
			"encryption_config": {"key": "test-only-key-do-not-use-0000000", "identifier": "local"},
			"cluster_config": %s,
			"store_config": {"reset": true, "adapters": {"unitdb": {"mem_size": 16777216}}}
		}`, n.tcpAddr, n.grpcAddr, clusterConf)
		confPath := filepath.Join(filepath.Dir(bin), confName)
		if err := os.WriteFile(confPath, []byte(conf), 0644); err != nil {
			t.Fatal(err)
		}
		n.args = []string{bin, "-config", confName, "-db_path", filepath.Join(dbDir, "db"), "-cluster_self", n.name}
		n := n
		t.Cleanup(func() {
			n.stop()
			os.Remove(confPath)
			os.RemoveAll(dbDir)
		})
		if err := n.start(); err != nil {
			t.Fatalf("start node %s: %v\n%s", n.name, err, n.logs.String())
		}
	}
	if _, err := c.waitLeader(c.nodes); err != nil {
		t.Fatal(err)
	}
	return c
}

func (n *clusterNode) start() error {
	n.cmd = exec.Command(n.args[0], n.args[1:]...)
	n.cmd.Stdout = n.logs
	n.cmd.Stderr = n.logs
	if err := n.cmd.Start(); err != nil {
		return err
	}
	n.exited = make(chan struct{})
	go func(cmd *exec.Cmd, exited chan struct{}) {
		cmd.Wait()
		close(exited)
	}(n.cmd, n.exited)

	deadline := time.Now().Add(15 * time.Second)
	for _, addr := range []string{n.tcpAddr, n.grpcAddr} {
		for {
			if !n.alive() {
				return fmt.Errorf("node %s exited", n.name)
			}
			if conn, err := dialTCP(addr); err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("node %s did not listen on %s", n.name, addr)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return nil
}

// stop kills the node, as a crash would.
func (n *clusterNode) stop() {
	if n.cmd != nil && n.cmd.Process != nil && n.alive() {
		n.cmd.Process.Kill()
		<-n.exited
	}
}

func (n *clusterNode) alive() bool {
	if n.exited == nil {
		return false
	}
	select {
	case <-n.exited:
		return false
	default:
		return true
	}
}

func (c *testCluster) others(n *clusterNode) []*clusterNode {
	var others []*clusterNode
	for _, o := range c.nodes {
		if o != n {
			others = append(others, o)
		}
	}
	return others
}

var (
	electedSelf = regexp.MustCompile(`Elected myself as a new leader`)
	leaderIs    = regexp.MustCompile(`leader (?:set to )?'([a-z]+)'(?: elected)?`)
)

// leaderSeen returns the leader a node last logged, or "" if none.
func (n *clusterNode) leaderSeen() string {
	leader := ""
	for _, line := range strings.Split(n.logs.String(), "\n") {
		if electedSelf.MatchString(line) {
			leader = n.name
		} else if m := leaderIs.FindStringSubmatch(line); m != nil && !strings.Contains(line, "wrong leader") {
			leader = m[1]
		}
	}
	return leader
}

// waitLeader waits until the live nodes agree on one leader.
func (c *testCluster) waitLeader(live []*clusterNode) (string, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		leaders := map[string]bool{}
		for _, n := range live {
			leaders[n.leaderSeen()] = true
		}
		if len(leaders) == 1 && !leaders[""] {
			for l := range leaders {
				return l, nil
			}
		}
		if time.Now().After(deadline) {
			seen := map[string]string{}
			for _, n := range live {
				seen[n.name] = n.leaderSeen()
			}
			return "", fmt.Errorf("nodes did not agree on a leader: %v", seen)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// kill stops n and waits until the other nodes have failed it over.
func (c *testCluster) kill(n *clusterNode) []*clusterNode {
	c.t.Helper()
	n.stop()
	live := c.others(n)
	// Failure detection takes node_fail_after heartbeats, then the ring is
	// rehashed and subscriptions move.
	time.Sleep(4 * time.Second)
	if _, err := c.waitLeader(live); err != nil {
		c.t.Fatalf("after %s died: %v", n.name, err)
	}
	return live
}

func (c *testCluster) assertAlive(nodes []*clusterNode, when string) {
	c.t.Helper()
	for _, n := range nodes {
		if !n.alive() {
			c.t.Fatalf("node %s died %s\nlogs:\n%s", n.name, when, n.logs.String())
		}
	}
}

func dialTCP(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 200*time.Millisecond)
}

// rawConnAt connects a raw connection to n with clientID and a fresh session.
func rawConnAt(t *testing.T, n *clusterNode, clientID string, insecure bool) *rawConn {
	t.Helper()
	c := dialRawAt(t, n.tcpAddr)
	c.send(&utp.Connect{ClientID: clientID, InsecureFlag: insecure, KeepAlive: 30, SessKey: int32(nextSessKey())})
	m := c.waitFor("connect acknowledge", func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.MessageType == utp.CONNECT
	})
	ack := &utp.ConnectAcknowledge{}
	ack.FromBinary(utp.FixedHeader{}, m.(*utp.ControlMessage).Message)
	if ack.ReturnCode != utp.Accepted {
		t.Fatalf("connect to %s: return code %d", n.name, ack.ReturnCode)
	}
	return c
}

// clusterClient connects the client to target, one of the cluster's nodes.
func clusterClient(t *testing.T, target, clientID string, opts ...Options) Client {
	t.Helper()
	return newE2EClient(t, target, clientID, opts...)
}

// topics returns clusterTopics topics under prefix.
func topics(prefix string) []string {
	ts := make([]string, clusterTopics)
	for i := range ts {
		ts[i] = fmt.Sprintf("%s.t%d", prefix, i)
	}
	return ts
}

// settle lets forwarded subscriptions reach their topics' owners.
func settle() { time.Sleep(200 * time.Millisecond) }

func TestClusterClientSubscribes(t *testing.T) {
	c := startCluster(t)
	clientID := newClientIDAt(t, c.nodes[0].tcpAddr)
	for _, n := range c.nodes {
		for _, transport := range []string{"tcp", "grpc"} {
			t.Run(n.name+"/"+transport, func(t *testing.T) {
				target := "tcp://" + n.tcpAddr
				if transport == "grpc" {
					target = "grpc://" + n.grpcAddr
				}
				prefix := fmt.Sprintf("cl.sub.%s.%s", n.name, transport)
				client := clusterClient(t, target, clientID)
				got := collect(t, client, prefix+"...")
				ts := topics(prefix)
				waitResult(t, "subscribe", client.SubscribeMultiple(ts))
				settle()

				// A publisher on every node, including the client's own.
				var want []string
				for _, pn := range c.nodes {
					pub := rawConnAt(t, pn, clientID, true)
					for i, topic := range ts {
						payload := fmt.Sprintf("%s>%s", pn.name, topic)
						pub.publish(uint16(i+1), topic, payload, 0)
						want = append(want, payload)
					}
				}
				expectPayloads(t, got, want...)
			})
		}
	}
	c.assertAlive(c.nodes, "while serving the client")
}

func TestClusterClientPublishes(t *testing.T) {
	c := startCluster(t)
	clientID := newClientIDAt(t, c.nodes[0].tcpAddr)
	for _, n := range c.nodes {
		t.Run(n.name, func(t *testing.T) {
			ts := topics("cl.pub." + n.name)
			// A subscriber on every node.
			subs := map[*clusterNode]*rawConn{}
			for _, sn := range c.nodes {
				sub := rawConnAt(t, sn, clientID, true)
				for i, topic := range ts {
					sub.subscribe(uint16(i+1), topic, 0)
				}
				subs[sn] = sub
			}
			settle()

			client := clusterClient(t, "tcp://"+n.tcpAddr, clientID)
			for _, topic := range ts {
				waitResult(t, "publish", client.Publish(topic, []byte(n.name+">"+topic)))
			}
			for sn, sub := range subs {
				for _, topic := range ts {
					want := n.name + ">" + topic
					sub.waitFor(fmt.Sprintf("%q on %s", want, sn.name), func(m lp.MessagePack) bool {
						pub, ok := m.(*utp.Publish)
						return ok && len(pub.Messages) > 0 && string(pub.Messages[0].Payload) == want
					})
				}
			}
		})
	}
	c.assertAlive(c.nodes, "while the client published")
}

func TestClusterClientReliableDelivery(t *testing.T) {
	c := startCluster(t)
	clientID := newClientIDAt(t, c.nodes[0].tcpAddr)
	for _, n := range c.nodes {
		t.Run(n.name, func(t *testing.T) {
			prefix := "cl.reliable." + n.name
			client := clusterClient(t, "tcp://"+n.tcpAddr, clientID)
			got := collect(t, client, prefix+"...")
			ts := topics(prefix)
			waitResult(t, "subscribe", client.SubscribeMultiple(ts, WithSubDeliveryMode(1)))
			settle()

			// Reliable publishes from the other nodes: the client receives
			// each through the notify, receive, receipt, complete flow.
			var want []string
			for _, pn := range c.others(n) {
				pub := rawConnAt(t, pn, clientID, true)
				for i, topic := range ts {
					payload := pn.name + ">" + topic
					pub.publish(uint16(i+1), topic, payload, 1)
					want = append(want, payload)
				}
			}
			expectPayloads(t, got, want...)
		})
	}
	c.assertAlive(c.nodes, "during reliable delivery")
}

func TestClusterClientRelay(t *testing.T) {
	c := startCluster(t)
	clientID := newClientIDAt(t, c.nodes[0].tcpAddr)
	for _, n := range c.nodes {
		t.Run(n.name, func(t *testing.T) {
			prefix := "cl.relay." + n.name
			ts := topics(prefix)
			// Messages stored in the cluster through the other nodes.
			var want []string
			for _, pn := range c.others(n) {
				pub := rawConnAt(t, pn, clientID, true)
				for i, topic := range ts {
					payload := pn.name + ">" + topic
					pub.send(&utp.Publish{MessageID: uint16(i + 1), Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte(payload), Ttl: "1h"}}})
					pub.waitFor("publish acknowledge", isControl(utp.PUBLISH, utp.ACKNOWLEDGE, uint16(i+1)))
					want = append(want, payload)
				}
			}

			client := clusterClient(t, "grpc://"+n.grpcAddr, clientID)
			got := collect(t, client, prefix+"...")
			waitResult(t, "relay", client.Relay(ts, WithLast("1h")))
			expectPayloads(t, got, want...)
		})
	}
}

func TestClusterClientSecurePubSub(t *testing.T) {
	c := startCluster(t)
	clientID := newClientIDAt(t, c.nodes[0].tcpAddr)
	for _, n := range c.nodes {
		t.Run(n.name, func(t *testing.T) {
			client := newE2EClientSecurity(t, "tcp://"+n.tcpAddr, clientID, false)

			// Keys from this node's keygen are valid on every node.
			responses := collect(t, client, "unitdb/keygen")
			topic := "cl.secure." + n.name
			waitResult(t, "keygen", client.Publish("unitdb/keygen", []byte(fmt.Sprintf(`[{"topic":%q,"type":"rw"}]`, topic))))
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

			got := collect(t, client, topic)
			waitResult(t, "subscribe", client.Subscribe(key+"/"+topic))
			settle()
			var want []string
			for _, pn := range c.nodes {
				pub := rawConnAt(t, pn, clientID, false)
				payload := pn.name + ">" + topic
				pub.publish(1, key+"/"+topic, payload, 0)
				want = append(want, payload)
			}
			expectPayloads(t, got, want...)
		})
	}
}

func TestClusterClientWildcardSubscription(t *testing.T) {
	c := startCluster(t)
	clientID := newClientIDAt(t, c.nodes[0].tcpAddr)
	for _, n := range c.nodes {
		t.Run(n.name, func(t *testing.T) {
			prefix := "cl.wild." + n.name
			client := clusterClient(t, "tcp://"+n.tcpAddr, clientID)
			got := collect(t, client, prefix+"...")
			// Every node holds a wildcard subscription.
			waitResult(t, "subscribe", client.Subscribe(prefix+"..."))
			settle()

			var want []string
			for _, pn := range c.nodes {
				pub := rawConnAt(t, pn, clientID, true)
				for i, topic := range topics(prefix + "." + pn.name) {
					payload := pn.name + ">" + topic
					pub.publish(uint16(i+1), topic, payload, 0)
					want = append(want, payload)
				}
			}
			expectPayloads(t, got, want...)
		})
	}
}

func TestClusterClientSurvivesNodeFailure(t *testing.T) {
	for _, victim := range []string{"leader", "follower"} {
		t.Run("kill "+victim, func(t *testing.T) {
			c := startCluster(t)
			leader, _ := c.waitLeader(c.nodes)
			var dead *clusterNode
			for _, n := range c.nodes {
				if (victim == "leader") == (n.name == leader) {
					dead = n
					break
				}
			}
			live := c.others(dead)
			clientNode, pubNode := live[0], live[1]
			clientID := newClientIDAt(t, clientNode.tcpAddr)

			lost := make(chan error, 1)
			client := clusterClient(t, "tcp://"+clientNode.tcpAddr, clientID,
				WithConnectionLostHandler(func(_ Client, err error) {
					select {
					case lost <- err:
					default:
					}
				}))
			got := collect(t, client, "cl.failover...")
			ts := topics("cl.failover")
			waitResult(t, "subscribe", client.SubscribeMultiple(ts))
			settle()

			check := func(when string) {
				t.Helper()
				pub := rawConnAt(t, pubNode, clientID, true)
				var want []string
				for i, topic := range ts {
					payload := when + ">" + topic
					pub.publish(uint16(i+1), topic, payload, 0)
					want = append(want, payload)
				}
				expectPayloads(t, got, want...)
			}
			check("before")

			// Some of the topics were owned by the dead node; its survivors
			// take them over with the client's subscriptions.
			c.kill(dead)
			select {
			case err := <-lost:
				t.Fatalf("client on %s lost its connection when %s died: %v", clientNode.name, dead.name, err)
			default:
			}
			check("after")
			c.assertAlive(live, "after "+dead.name+" died")
		})
	}
}

func TestClusterClientResumesSessionOnAnotherNode(t *testing.T) {
	c := startCluster(t)
	dead := c.nodes[0]
	live := c.others(dead)
	clientID := newClientIDAt(t, live[0].tcpAddr)
	sessKey := nextSessKey()
	const n = 5
	topic := "cl.session"

	// A subscriber of the session on the node about to die, notified of
	// reliable messages it does not receive yet.
	held := dialRawAt(t, dead.tcpAddr)
	held.send(&utp.Connect{ClientID: clientID, InsecureFlag: true, KeepAlive: 30, SessKey: int32(sessKey)})
	held.waitFor("connect acknowledge", func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.MessageType == utp.CONNECT
	})
	held.subscribe(1, topic, 1)
	settle()

	pub := rawConnAt(t, live[1], clientID, true)
	for i := 0; i < n; i++ {
		pub.send(&utp.Publish{MessageID: uint16(10 + i), DeliveryMode: 1, Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte(fmt.Sprintf("r%d", i)), Ttl: "1h"}}})
		pub.waitFor("publish acknowledge", isControl(utp.PUBLISH, utp.ACKNOWLEDGE, uint16(10+i)))
	}
	for i := 0; i < n; i++ {
		held.waitFor("notify", func(m lp.MessagePack) bool {
			ctrl, ok := m.(*utp.ControlMessage)
			return ok && ctrl.FlowControl == utp.NOTIFY
		})
	}
	time.Sleep(500 * time.Millisecond) // session logs replicate asynchronously

	held.conn.Close()
	c.kill(dead)

	// The client resumes the session on a surviving node, with the dead node
	// first in its server list (NewClient adds its target after the
	// AddServer servers), and receives the messages.
	client := clusterClient(t, "tcp://"+live[0].tcpAddr, clientID,
		AddServer("tcp://"+dead.tcpAddr),
		WithSessionKey(sessKey),
		WithConnectTimeout(2*time.Second))
	got := collect(t, client, topic)
	var want []string
	for i := 0; i < n; i++ {
		want = append(want, fmt.Sprintf("r%d", i))
	}
	expectPayloads(t, got, want...)
	c.assertAlive(live, "while resuming the session")
}
