package broker

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
)

// Client is one TCP connection to the broker. The same client can publish
// and subscribe.
type Client struct {
	id   uint64
	conn net.Conn

	// Two goroutines write to a client: its reader (sending OK/ERR replies)
	// and its writer (sending messages). The lock stops their bytes from
	// getting mixed together.
	writeMu sync.Mutex
	w       *bufio.Writer

	// out holds messages waiting to be written to this client.
	out outbox

	// done is closed when the client goes away, so anything waiting on it
	// (a blocked publisher, the writer goroutine) can stop.
	done      chan struct{}
	closeOnce sync.Once

	delivered atomic.Int64 // messages written to the socket
	dropped   atomic.Int64 // messages thrown away because the queue was full

	// topics this client is subscribed to, so we can clean up on disconnect.
	// Only touched while holding the Broker's mu.
	topics map[string]struct{}
}

func newClient(id uint64, conn net.Conn, out outbox) *Client {
	return &Client{
		id:     id,
		conn:   conn,
		w:      bufio.NewWriter(conn),
		out:    out,
		done:   make(chan struct{}),
		topics: make(map[string]struct{}),
	}
}

// close is safe to call more than once and from any goroutine.
func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.conn.Close()
	})
}

// send writes a reply (OK/ERR) straight to the socket.
func (c *Client) send(line string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeLocked(line)
}

// writeLocked expects writeMu to be held.
func (c *Client) writeLocked(line string) error {
	if _, err := c.w.WriteString(line); err != nil {
		return err
	}
	return c.w.Flush()
}

// deliver hands a published message to this client's queue. It runs in
// the publisher's goroutine; what happens when the queue is full depends
// on the policy.
func (c *Client) deliver(line string) {
	if !c.out.push(line, c.done) {
		c.dropped.Add(1)
	}
}

// writeLoop runs in its own goroutine for each client. It's the only
// place that waits on a slow socket, so a slow subscriber only ever
// blocks its own writer, never the publisher directly.
func (c *Client) writeLoop() {
	for {
		batch, ok := c.out.pop(c.done)
		if !ok {
			return
		}

		c.writeMu.Lock()
		var err error
		for _, line := range batch {
			if _, err = c.w.WriteString(line); err != nil {
				break
			}
		}
		if err == nil {
			err = c.w.Flush()
		}
		c.writeMu.Unlock()

		if err != nil {
			c.close() // also makes the reader goroutine exit and clean up
			return
		}
		c.delivered.Add(int64(len(batch)))
	}
}
