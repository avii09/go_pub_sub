// Package benchmarks measures how fast the broker can publish with each
// policy when every subscriber keeps up.
//
//	go test -bench . ./experiments/benchmarks
package benchmarks

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/avii09/go_pub_sub/broker"
)

const numSubscribers = 3

// BenchmarkPublish measures one Publish call fanning out to 3 subscribers
// that read as fast as they can over TCP.
//
// The publisher here is an in-memory loop, faster than any real client,
// so under "drop" even fast subscribers can't always keep up. The
// dropped/op metric shows how often that happens.
func BenchmarkPublish(b *testing.B) {
	for _, policy := range []broker.Policy{broker.PolicyDrop, broker.PolicyBuffer, broker.PolicyBlock} {
		b.Run(string(policy), func(b *testing.B) {
			br, addr := startBroker(b, policy)
			for range numSubscribers {
				subscribe(b, addr, "bench")
			}
			payload := fmt.Sprintf("%0256d", 0) // 256-byte message

			for b.Loop() { // b.Loop only times what's inside the loop
				br.Publish("bench", payload)
			}

			var dropped int64
			for _, c := range br.Stats().Clients {
				dropped += c.Dropped
			}
			b.ReportMetric(float64(dropped)/float64(b.N*numSubscribers), "dropped/op")
		})
	}
}

func startBroker(b *testing.B, policy broker.Policy) (*broker.Broker, string) {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	br := broker.New(slog.New(slog.DiscardHandler), broker.Config{Policy: policy})
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := br.Serve(ctx, ln); err != nil {
			b.Errorf("serve: %v", err)
		}
	}()
	b.Cleanup(func() {
		cancel()
		<-served
	})
	return br, ln.Addr().String()
}

// subscribe connects a subscriber that reads and discards everything.
func subscribe(b *testing.B, addr, topic string) {
	b.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(func() { conn.Close() })

	r := bufio.NewReader(conn)
	fmt.Fprintf(conn, "SUB %s\n", topic)
	if reply, err := r.ReadString('\n'); err != nil || reply != "OK\n" {
		b.Fatalf("subscribe: reply %q, err %v", reply, err)
	}
	go io.Copy(io.Discard, r)
}
