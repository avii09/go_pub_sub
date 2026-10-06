package broker

import (
	"fmt"
	"slices"
	"sync"
)

// Policy decides what the broker does when a subscriber can't keep up.
// These are the three options from Designing Data-Intensive Applications:
// drop messages, buffer them in a queue, or apply backpressure.
type Policy string

const (
	// PolicyDrop: each subscriber has a fixed-size queue. When it's full,
	// new messages for that subscriber are thrown away.
	PolicyDrop Policy = "drop"

	// PolicyBuffer: each subscriber has a queue with no limit. Nothing is
	// lost and nobody waits, but memory grows while the subscriber is behind.
	PolicyBuffer Policy = "buffer"

	// PolicyBlock (backpressure): each subscriber has a fixed-size queue.
	// When it's full, the publisher waits until there's room.
	PolicyBlock Policy = "block"
)

func ParsePolicy(s string) (Policy, error) {
	switch p := Policy(s); p {
	case PolicyDrop, PolicyBuffer, PolicyBlock:
		return p, nil
	default:
		return "", fmt.Errorf("unknown policy %q (want drop, buffer or block)", s)
	}
}

// maxBatch caps how many queued messages the writer sends per flush.
const maxBatch = 256

// outbox is a subscriber's queue of messages waiting to be written to its
// socket. The broker pushes; the subscriber's writer goroutine pops.
type outbox interface {
	// push queues a message. It returns false if the message was dropped
	// because the queue was full.
	push(line string, done <-chan struct{}) bool

	// pop waits for queued messages and returns them as a batch.
	// It returns ok=false once done is closed (the client went away).
	pop(done <-chan struct{}) (batch []string, ok bool)

	// len is how many messages are waiting right now.
	len() int
}

func newOutbox(p Policy, size int) outbox {
	switch p {
	case PolicyDrop:
		return &chanOutbox{ch: make(chan string, size), dropWhenFull: true}
	case PolicyBuffer:
		return &sliceOutbox{ready: make(chan struct{}, 1)}
	default: // PolicyBlock
		return &chanOutbox{ch: make(chan string, size)}
	}
}

// chanOutbox is a fixed-size queue built on a buffered channel. Drop and
// block differ in one place only: what push does when the channel is full.
type chanOutbox struct {
	ch           chan string
	dropWhenFull bool
}

func (o *chanOutbox) push(line string, done <-chan struct{}) bool {
	if o.dropWhenFull {
		// DROP: try to send; if the queue is full, give up straight away.
		select {
		case o.ch <- line:
			return true
		default:
			return false
		}
	}

	// BLOCK: wait for room. Also watch done, otherwise a publisher could
	// wait forever on a subscriber that has already disconnected.
	select {
	case o.ch <- line:
	case <-done:
	}
	return true
}

func (o *chanOutbox) pop(done <-chan struct{}) ([]string, bool) {
	var first string
	select {
	case first = <-o.ch:
	case <-done:
		return nil, false
	}

	// Grab whatever else is already waiting so we can write it in one go.
	batch := []string{first}
	for len(batch) < maxBatch {
		select {
		case line := <-o.ch:
			batch = append(batch, line)
		default:
			return batch, true
		}
	}
	return batch, true
}

func (o *chanOutbox) len() int { return len(o.ch) }

// sliceOutbox is the BUFFER policy: a queue with no size limit. A channel
// can't do this (its size is fixed when you make it), so it's a slice
// protected by a mutex.
type sliceOutbox struct {
	mu    sync.Mutex
	items []string
	ready chan struct{} // wakes the writer when items is no longer empty
}

func (o *sliceOutbox) push(line string, done <-chan struct{}) bool {
	o.mu.Lock()
	o.items = append(o.items, line)
	o.mu.Unlock()

	// Wake the writer if it's waiting. If a wake-up is already pending,
	// that one is enough.
	select {
	case o.ready <- struct{}{}:
	default:
	}
	return true // never drops: that's the point, and the problem
}

func (o *sliceOutbox) pop(done <-chan struct{}) ([]string, bool) {
	for {
		o.mu.Lock()
		if n := len(o.items); n > 0 {
			k := min(n, maxBatch)
			batch := slices.Clone(o.items[:k])
			clear(o.items[:k]) // so the old backing array doesn't keep these alive
			o.items = o.items[k:]
			o.mu.Unlock()
			return batch, true
		}
		o.mu.Unlock()

		select {
		case <-o.ready:
		case <-done:
			return nil, false
		}
	}
}

func (o *sliceOutbox) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.items)
}
