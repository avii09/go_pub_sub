package main

import (
	"bufio"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// subscriber connects to the broker, subscribes to a topic and reads
// messages. A slow subscriber sleeps after every message to simulate
// expensive processing (a database write, a slow API call...).
type subscriber struct {
	name  string
	conn  net.Conn
	delay time.Duration // processing time per message; 0 = fast

	received atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration // since the last snapshot
}

func newSubscriber(addr, topic, name string, delay time.Duration) (*subscriber, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s dial: %w", name, err)
	}

	// Wait for the broker's OK so we know we're subscribed before the
	// publisher starts.
	r := bufio.NewReader(conn)
	fmt.Fprintf(conn, "SUB %s\n", topic)
	reply, err := r.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("%s subscribe: %w", name, err)
	}
	if strings.TrimSpace(reply) != "OK" {
		conn.Close()
		return nil, fmt.Errorf("%s subscribe: broker replied %q", name, strings.TrimSpace(reply))
	}

	s := &subscriber{name: name, conn: conn, delay: delay}
	go s.read(r)
	return s, nil
}

func (s *subscriber) read(r *bufio.Reader) {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return // connection closed: by us on shutdown, or by the broker
		}
		s.handle(line)
		if s.delay > 0 {
			time.Sleep(s.delay)
		}
	}
}

// handle parses "MSG <topic> <seq> <sentAtNanos> <padding>" and records
// how long the message took to arrive.
func (s *subscriber) handle(line string) {
	fields := strings.Fields(line)
	if len(fields) < 4 || fields[0] != "MSG" {
		return // not a message we produced; ignore it
	}
	sentAt, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return
	}
	s.received.Add(1)

	lat := time.Since(time.Unix(0, sentAt))
	s.mu.Lock()
	s.latencies = append(s.latencies, lat)
	s.mu.Unlock()
}

// takeLatencies returns the p50 and p99 latency since the last call,
// and resets the sample.
func (s *subscriber) takeLatencies() (p50, p99 time.Duration, ok bool) {
	s.mu.Lock()
	lats := s.latencies
	s.latencies = nil
	s.mu.Unlock()

	if len(lats) == 0 {
		return 0, 0, false
	}
	slices.Sort(lats)
	return lats[len(lats)*50/100], lats[len(lats)*99/100], true
}

func (s *subscriber) close() { s.conn.Close() }
