package broker

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
)

// maxLineBytes caps a single command. bufio.Scanner's default is 64 KB;
// anything longer is treated as a protocol error and the client is dropped.
const maxLineBytes = 64 * 1024

// DefaultQueueSize is the per-subscriber queue length for the drop and
// block policies.
const DefaultQueueSize = 1024

type Config struct {
	Policy    Policy
	QueueSize int // ignored by the buffer policy, which has no limit
}

// Broker routes published messages to every subscriber of a topic.
type Broker struct {
	log *slog.Logger
	cfg Config

	// mu guards topics, clients and each client's topics. Many goroutines
	// (one per connection) read and change these maps at the same time.
	mu      sync.RWMutex
	topics  map[string]*Topic
	clients map[*Client]struct{}

	nextID    atomic.Uint64
	published atomic.Int64
	wg        sync.WaitGroup // tracks connection goroutines for clean shutdown
}

func New(log *slog.Logger, cfg Config) *Broker {
	if cfg.Policy == "" {
		cfg.Policy = PolicyBlock
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	return &Broker{
		log:     log,
		cfg:     cfg,
		topics:  make(map[string]*Topic),
		clients: make(map[*Client]struct{}),
	}
}

// Serve accepts connections until ctx is cancelled, then closes every
// connection and waits for their goroutines to finish.
func (b *Broker) Serve(ctx context.Context, ln net.Listener) error {
	// Accept blocks, so we close the listener from another goroutine to
	// unblock it when it's time to shut down.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				b.closeAll()
				b.wg.Wait()
				return nil
			}
			return err
		}
		b.wg.Go(func() { b.handleConn(conn) })
	}
}

// handleConn runs in its own goroutine for each client: read a line,
// act on it, repeat until the client goes away.
func (b *Broker) handleConn(conn net.Conn) {
	c := newClient(b.nextID.Add(1), conn, newOutbox(b.cfg.Policy, b.cfg.QueueSize))
	log := b.log.With("client", c.id, "remote", conn.RemoteAddr().String())

	b.mu.Lock()
	b.clients[c] = struct{}{}
	b.mu.Unlock()
	log.Info("client connected")

	b.wg.Go(c.writeLoop)

	defer func() {
		b.removeClient(c)
		c.close()
		log.Info("client disconnected")
	}()

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 4096), maxLineBytes)

	for sc.Scan() {
		cmd, err := ParseCommand(sc.Text())
		if errors.Is(err, ErrEmptyCommand) {
			continue
		}
		if err != nil {
			c.send("ERR " + err.Error() + "\n")
			continue
		}

		switch cmd.Op {
		case OpSub:
			// Hold the write lock while subscribing so the OK reaches the
			// client before any message on the new topic can be written.
			c.writeMu.Lock()
			b.subscribe(c, cmd.Topic)
			c.writeLocked("OK\n")
			c.writeMu.Unlock()
		case OpUnsub:
			b.unsubscribe(c, cmd.Topic)
			c.send("OK\n")
		case OpPub:
			// No reply to PUB on purpose. A fast publisher that never reads
			// its replies would fill its own receive buffer, and the broker
			// would block writing "OK"s back to it: backpressure we didn't ask for.
			b.Publish(cmd.Topic, cmd.Payload)
		}
	}

	// A clean close ends the loop with a nil error. A reset just means the
	// client went away with unread data, which is a normal disconnect too.
	// Anything else (like a line over maxLineBytes) is worth logging.
	if err := sc.Err(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.ECONNRESET) {
		log.Warn("read failed", "err", err)
	}
}

func (b *Broker) subscribe(c *Client, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics[topic]
	if !ok {
		t = newTopic(topic)
		b.topics[topic] = t
	}
	t.add(c)
	c.topics[topic] = struct{}{}
}

func (b *Broker) unsubscribe(c *Client, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unsubscribeLocked(c, topic)
}

// unsubscribeLocked expects b.mu to be held.
func (b *Broker) unsubscribeLocked(c *Client, topic string) {
	delete(c.topics, topic)
	t, ok := b.topics[topic]
	if !ok {
		return
	}
	t.remove(c)
	if t.empty() {
		delete(b.topics, topic) // don't let abandoned topics pile up
	}
}

// removeClient drops every subscription a client had. Without this, the
// broker would keep trying to deliver to a dead connection.
func (b *Broker) removeClient(c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic := range c.topics {
		b.unsubscribeLocked(c, topic)
	}
	delete(b.clients, c)
}

// Publish delivers payload to every current subscriber of topic. What
// happens when a subscriber is behind depends on the policy; see
// Client.deliver.
func (b *Broker) Publish(topic, payload string) {
	b.published.Add(1)

	b.mu.RLock()
	t, ok := b.topics[topic]
	var subs []*Client
	if ok {
		subs = t.snapshot()
	}
	b.mu.RUnlock()

	if len(subs) == 0 {
		return // no subscribers: the message is simply gone, like in NATS core
	}

	line := FormatMessage(topic, payload)
	for _, c := range subs {
		c.deliver(line)
	}
}

// closeAll closes every open connection during shutdown, which ends each
// handleConn loop and writer goroutine.
func (b *Broker) closeAll() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for c := range b.clients {
		c.close()
	}
}
