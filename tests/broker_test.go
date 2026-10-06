package tests

import (
	"testing"
	"time"

	"github.com/avii09/go_pub_sub/broker"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		line    string
		want    broker.Command
		wantErr bool
	}{
		{line: "SUB dogs", want: broker.Command{Op: "SUB", Topic: "dogs"}},
		{line: "unsub dogs", want: broker.Command{Op: "UNSUB", Topic: "dogs"}},
		{line: "PUB dogs hello world", want: broker.Command{Op: "PUB", Topic: "dogs", Payload: "hello world"}},
		{line: "PUB dogs hi\r", want: broker.Command{Op: "PUB", Topic: "dogs", Payload: "hi"}},
		{line: "SUB", wantErr: true},
		{line: "PUB dogs", wantErr: true},
		{line: "HELLO", wantErr: true},
		{line: "   ", wantErr: true},
	}
	for _, tt := range tests {
		got, err := broker.ParseCommand(tt.line)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseCommand(%q) error = %v, wantErr %v", tt.line, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseCommand(%q) = %+v, want %+v", tt.line, got, tt.want)
		}
	}
}

func TestSubscriberReceivesPublishedMessage(t *testing.T) {
	_, addr := startBroker(t, broker.PolicyBlock, 0)
	sub := dial(t, addr)
	pub := dial(t, addr)

	sub.subscribe("dogs")
	pub.send("PUB dogs hello world")

	sub.expect("MSG dogs hello world")
}

func TestOnlySubscribersOfTheTopicReceive(t *testing.T) {
	_, addr := startBroker(t, broker.PolicyBlock, 0)
	sub := dial(t, addr)
	pub := dial(t, addr)

	sub.subscribe("dogs")
	pub.send("PUB cats meow")
	pub.send("PUB dogs woof")

	// If the cats message had leaked through, it would arrive first.
	sub.expect("MSG dogs woof")
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	_, addr := startBroker(t, broker.PolicyBlock, 0)
	sub := dial(t, addr)
	pub := dial(t, addr)

	sub.subscribe("dogs")
	sub.send("UNSUB dogs")
	sub.expect("OK")
	sub.subscribe("cats")

	pub.send("PUB dogs woof")
	pub.send("PUB cats meow")

	sub.expect("MSG cats meow")
}

func TestInvalidCommandGetsAnError(t *testing.T) {
	_, addr := startBroker(t, broker.PolicyBlock, 0)
	c := dial(t, addr)

	c.send("HELLO")
	c.expect(`ERR unknown command "HELLO"`)

	// The connection still works afterwards.
	c.subscribe("dogs")
}

func TestDisconnectedClientIsCleanedUp(t *testing.T) {
	b, addr := startBroker(t, broker.PolicyBlock, 0)
	sub := dial(t, addr)
	sub.subscribe("dogs")

	if n := len(b.Stats().Clients); n != 1 {
		t.Fatalf("clients before disconnect = %d, want 1", n)
	}
	sub.conn.Close()

	eventually(t, 2*time.Second, "client still registered after disconnect", func() bool {
		return len(b.Stats().Clients) == 0
	})
	// Publishing to the now-empty topic must not fail or block.
	b.Publish("dogs", "anyone there?")
}
