package tests

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avii09/go_pub_sub/broker"
)

// startBroker runs a broker on a random free port and stops it when the
// test ends.
func startBroker(t *testing.T, policy broker.Policy, queueSize int) (*broker.Broker, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b := broker.New(slog.New(slog.DiscardHandler), broker.Config{Policy: policy, QueueSize: queueSize})

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- b.Serve(ctx, ln) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("broker did not shut down within 5s")
		}
	})
	return b, ln.Addr().String()
}

// testClient is a raw TCP connection to the broker.
type testClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *testClient) send(line string) {
	c.t.Helper()
	if _, err := c.conn.Write([]byte(line + "\n")); err != nil {
		c.t.Fatalf("write %q: %v", line, err)
	}
}

// readLine waits up to 2s for the next line from the broker.
func (c *testClient) readLine() string {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return strings.TrimSuffix(line, "\n")
}

func (c *testClient) expect(want string) {
	c.t.Helper()
	if got := c.readLine(); got != want {
		c.t.Fatalf("got %q, want %q", got, want)
	}
}

// subscribe sends SUB and waits for the broker's OK.
func (c *testClient) subscribe(topic string) {
	c.t.Helper()
	c.send("SUB " + topic)
	c.expect("OK")
}

// countMessages reads in the background and counts every MSG line, like
// a fast subscriber would.
func (c *testClient) countMessages() *atomic.Int64 {
	var n atomic.Int64
	go func() {
		for {
			line, err := c.r.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "MSG ") {
				n.Add(1)
			}
		}
	}()
	return &n
}

// clientStats finds a client in the broker's stats by its address.
func clientStats(b *broker.Broker, c *testClient) (broker.ClientStats, bool) {
	for _, cs := range b.Stats().Clients {
		if cs.Remote == c.conn.LocalAddr().String() {
			return cs, true
		}
	}
	return broker.ClientStats{}, false
}

// eventually retries cond until it's true or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v: %s", timeout, msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
