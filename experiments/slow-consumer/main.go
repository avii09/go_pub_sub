// Command slow-consumer is the live demo: one publisher, some fast
// subscribers and one slow subscriber, all talking to a running broker
// over TCP. Every second it prints how each of them is doing.
//
//	go run ./cmd/broker -policy drop          # terminal 1
//	go run ./experiments/slow-consumer        # terminal 2
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

type config struct {
	addr      string
	statsURL  string
	topic     string
	rate      int
	fast      int
	slowDelay time.Duration
	payload   int
	duration  time.Duration
	plain     bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "localhost:7777", "broker address")
	flag.StringVar(&cfg.statsURL, "stats", "http://localhost:7778/stats", "broker stats endpoint (empty to skip)")
	flag.StringVar(&cfg.topic, "topic", "demo", "topic to publish and subscribe on")
	flag.IntVar(&cfg.rate, "rate", 1000, "messages per second the publisher tries to send")
	flag.IntVar(&cfg.fast, "fast", 2, "number of fast subscribers")
	flag.DurationVar(&cfg.slowDelay, "slow-delay", 100*time.Millisecond, "processing time per message for the slow subscriber (0 = no slow subscriber)")
	flag.IntVar(&cfg.payload, "payload", 256, "padding bytes per message")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "how long to run")
	flag.BoolVar(&cfg.plain, "plain", false, "print one block per second instead of redrawing the screen")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	var subs []*subscriber
	defer func() {
		for _, s := range subs {
			s.close()
		}
	}()

	for i := range cfg.fast {
		s, err := newSubscriber(cfg.addr, cfg.topic, fmt.Sprintf("fast-%c", 'A'+i), 0)
		if err != nil {
			return err
		}
		subs = append(subs, s)
	}
	if cfg.slowDelay > 0 {
		s, err := newSubscriber(cfg.addr, cfg.topic, "SLOW", cfg.slowDelay)
		if err != nil {
			return err
		}
		subs = append(subs, s)
	}

	pub, err := newPublisher(cfg.addr, cfg.topic, cfg.rate, cfg.payload)
	if err != nil {
		return err
	}
	defer pub.conn.Close()

	pubErr := make(chan error, 1)
	go func() { pubErr <- pub.run(ctx) }()

	d := newDashboard(cfg, pub, subs)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Close our end so a publisher stuck in a blocked write returns.
			pub.conn.Close()
			<-pubErr
			// Give fast subscribers a moment to receive what's in flight.
			time.Sleep(500 * time.Millisecond)
			d.summary()
			return nil
		case err := <-pubErr:
			return err
		case <-ticker.C:
			d.tick()
		}
	}
}
