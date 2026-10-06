package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/avii09/go_pub_sub/broker"
)

// dashboard turns the raw counters into per-second rates and prints them.
type dashboard struct {
	cfg   config
	start time.Time
	pub   *publisher
	subs  []*subscriber

	lastSent int64
	lastRecv []int64
}

func newDashboard(cfg config, pub *publisher, subs []*subscriber) *dashboard {
	return &dashboard{
		cfg:      cfg,
		start:    time.Now(),
		pub:      pub,
		subs:     subs,
		lastRecv: make([]int64, len(subs)),
	}
}

// tick prints one second's worth of numbers.
func (d *dashboard) tick() {
	var b strings.Builder
	if !d.cfg.plain {
		b.WriteString("\033[H\033[2J") // move cursor home and clear screen
	}

	fmt.Fprintf(&b, "t=%-4s publishing %d msg/s   slow subscriber: %v per message\n",
		time.Since(d.start).Round(time.Second), d.cfg.rate, d.cfg.slowDelay)

	// What the broker sees from the inside. If stats aren't available we
	// still show what the clients see.
	var inside map[string]broker.ClientStats
	if d.cfg.statsURL != "" {
		if st, err := fetchStats(d.cfg.statsURL); err != nil {
			fmt.Fprintf(&b, "broker: stats unavailable (%v)\n", err)
		} else {
			fmt.Fprintf(&b, "broker: policy=%s  queue=%d  memory=%.1f MB\n",
				st.Policy, st.QueueSize, float64(st.HeapBytes)/(1<<20))
			inside = clientByAddr(st)
		}
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "%-10s %9s %10s %10s %10s %10s %10s\n",
		"", "msg/s", "total", "dropped", "queued", "p50 lat", "p99 lat")

	sent := d.pub.sent.Load()
	fmt.Fprintf(&b, "%-10s %9d %10d\n", "publisher", sent-d.lastSent, sent)
	d.lastSent = sent

	for i, s := range d.subs {
		recv := s.received.Load()
		p50, p99, ok := s.takeLatencies()
		lat50, lat99 := "-", "-"
		if ok {
			lat50, lat99 = fmtDur(p50), fmtDur(p99)
		}
		// dropped and queued come from the broker.
		dropped, queued := "-", "-"
		if c, found := inside[s.conn.LocalAddr().String()]; found {
			dropped, queued = fmt.Sprint(c.Dropped), fmt.Sprint(c.Queued)
		}
		fmt.Fprintf(&b, "%-10s %9d %10d %10s %10s %10s %10s\n",
			s.name, recv-d.lastRecv[i], recv, dropped, queued, lat50, lat99)
		d.lastRecv[i] = recv
	}
	b.WriteString("\n")
	fmt.Print(b.String())
}

func (d *dashboard) summary() {
	sent := d.pub.sent.Load()
	elapsed := time.Since(d.start).Seconds()
	fmt.Printf("\n=== summary after %.0fs ===\n", elapsed)
	fmt.Printf("publisher sent %d (%.0f msg/s average, target %d)\n", sent, float64(sent)/elapsed, d.cfg.rate)

	var inside map[string]broker.ClientStats
	if d.cfg.statsURL != "" {
		if st, err := fetchStats(d.cfg.statsURL); err == nil {
			inside = clientByAddr(st)
		}
	}
	for _, s := range d.subs {
		recv := s.received.Load()
		line := fmt.Sprintf("%-10s received %d (%.0f%%)", s.name, recv, pct(recv, sent))
		if c, found := inside[s.conn.LocalAddr().String()]; found {
			line += fmt.Sprintf(", dropped %d, still queued in broker %d", c.Dropped, c.Queued)
		}
		fmt.Println(line)
	}
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	default:
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}
