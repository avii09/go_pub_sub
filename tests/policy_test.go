package tests

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avii09/go_pub_sub/broker"
)

// Each test has one fast subscriber that reads everything and one slow
// subscriber that never reads at all. Messages are big (16 KB) so the
// slow subscriber's TCP buffers fill quickly: the OS can quietly hold
// several MB per connection before the broker's own queue even matters.
const (
	numMessages = 1000
	queueSize   = 16
)

var bigPayload = strings.Repeat("x", 16*1024)

// setup starts a broker with the given policy plus a fast and a slow
// subscriber on topic "t".
func setup(t *testing.T, policy broker.Policy) (b *broker.Broker, fastCount *atomic.Int64, slow *testClient) {
	t.Helper()
	b, addr := startBroker(t, policy, queueSize)

	fast := dial(t, addr)
	fast.subscribe("t")
	fastCount = fast.countMessages()

	slow = dial(t, addr)
	slow.subscribe("t") // ...and then never read again

	return b, fastCount, slow
}

// publishAll publishes numMessages in the background. The returned
// counter shows how far it got; done is closed when it finishes.
//
// It sends one message every 100µs, like a real publisher with a steady
// rate. Without a pause, even the fast subscriber's small queue would
// overflow under the drop policy, since nothing can read as fast as an
// in-memory loop can publish.
func publishAll(b *broker.Broker) (published *atomic.Int64, done chan struct{}) {
	published = new(atomic.Int64)
	done = make(chan struct{})
	go func() {
		defer close(done)
		for range numMessages {
			b.Publish("t", bigPayload)
			published.Add(1)
			time.Sleep(100 * time.Microsecond)
		}
	}()
	return published, done
}

func waitDone(t *testing.T, done chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// DROP: the publisher never waits and the fast subscriber gets everything,
// but the slow subscriber loses messages.
func TestDrop_slowSubscriberLosesMessages_othersUnaffected(t *testing.T) {
	b, fastCount, slow := setup(t, broker.PolicyDrop)

	_, done := publishAll(b)
	waitDone(t, done, "publisher was blocked; drop should never block")

	eventually(t, 5*time.Second, "fast subscriber didn't get every message", func() bool {
		return fastCount.Load() == numMessages
	})

	cs, ok := clientStats(b, slow)
	if !ok {
		t.Fatal("slow subscriber not found in stats")
	}
	if cs.Dropped == 0 {
		t.Errorf("slow subscriber dropped = 0, want > 0")
	}
	if cs.Queued > queueSize {
		t.Errorf("slow subscriber queued = %d, want <= %d (queue is bounded)", cs.Queued, queueSize)
	}
}

// BUFFER: nobody waits and nothing is lost, but messages pile up in the
// broker's memory for the slow subscriber.
func TestBuffer_nothingLost_butQueueGrows(t *testing.T) {
	b, fastCount, slow := setup(t, broker.PolicyBuffer)

	_, done := publishAll(b)
	waitDone(t, done, "publisher was blocked; buffer should never block")

	eventually(t, 5*time.Second, "fast subscriber didn't get every message", func() bool {
		return fastCount.Load() == numMessages
	})

	cs, ok := clientStats(b, slow)
	if !ok {
		t.Fatal("slow subscriber not found in stats")
	}
	if cs.Dropped != 0 {
		t.Errorf("slow subscriber dropped = %d, want 0", cs.Dropped)
	}
	if cs.Queued <= queueSize {
		// The buffer policy has no limit, so it should hold far more than
		// a bounded queue would.
		t.Errorf("slow subscriber queued = %d, want it to grow past %d", cs.Queued, queueSize)
	}
}

// BLOCK: nothing is lost, but the slow subscriber stalls the publisher,
// and so the fast subscriber stops getting messages too.
func TestBlock_slowSubscriberStallsEveryone(t *testing.T) {
	b, fastCount, slow := setup(t, broker.PolicyBlock)

	published, done := publishAll(b)

	// Wait until the publisher stops making progress.
	var last int64 = -1
	eventually(t, 5*time.Second, "publisher never stalled", func() bool {
		time.Sleep(200 * time.Millisecond)
		now := published.Load()
		stalled := now == last
		last = now
		return stalled
	})
	if last >= numMessages {
		t.Fatalf("publisher finished all %d messages; block should have stalled it", numMessages)
	}
	if got := fastCount.Load(); got >= numMessages {
		t.Errorf("fast subscriber got %d messages; it should be held back too", got)
	}
	if cs, ok := clientStats(b, slow); !ok || cs.Dropped != 0 {
		t.Errorf("slow subscriber stats = %+v (found=%v), want 0 dropped", cs, ok)
	}

	// Once the slow subscriber leaves, the publisher must be released
	// instead of waiting forever.
	slow.conn.Close()
	waitDone(t, done, "publisher still blocked after the slow subscriber disconnected")
	eventually(t, 5*time.Second, "fast subscriber didn't catch up", func() bool {
		return fastCount.Load() == numMessages
	})
}
