package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// publisher sends messages to the broker at a target rate and counts how
// many it actually managed to send. If the broker stops reading from us,
// our writes block and the "sent" rate drops below the target: that's
// backpressure reaching the producer.
type publisher struct {
	conn    net.Conn
	topic   string
	rate    int
	padding string

	sent atomic.Int64 // also the sequence number of the last message sent
}

func newPublisher(addr, topic string, rate, payloadBytes int) (*publisher, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("publisher dial: %w", err)
	}
	return &publisher{
		conn:    conn,
		topic:   topic,
		rate:    rate,
		padding: strings.Repeat("x", payloadBytes),
	}, nil
}

// run publishes until ctx is cancelled. Every 10ms it sends rate/100
// messages. If a batch takes longer than 10ms (because writes block),
// the ticker skips ticks instead of catching up in a burst, so the
// sent rate honestly shows how much we were slowed down.
func (p *publisher) run(ctx context.Context) error {
	const ticksPerSecond = 100
	perTick := max(p.rate/ticksPerSecond, 1)

	w := bufio.NewWriter(p.conn)
	ticker := time.NewTicker(time.Second / ticksPerSecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		for range perTick {
			seq := p.sent.Load() + 1
			// Payload: "<seq> <send time in ns> <padding>". Subscribers use
			// the timestamp to measure latency.
			fmt.Fprintf(w, "PUB %s %d %d %s\n", p.topic, seq, time.Now().UnixNano(), p.padding)
			p.sent.Store(seq)
		}
		if err := w.Flush(); err != nil {
			if ctx.Err() != nil {
				return nil // we closed the connection ourselves on shutdown
			}
			return fmt.Errorf("publisher write: %w", err)
		}
	}
}
