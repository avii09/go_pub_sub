package broker

import (
	"cmp"
	"encoding/json"
	"net/http"
	"runtime"
	"slices"
)

// Stats is a point-in-time view of the broker, served as JSON on /stats so
// the demo dashboard can show what's happening inside.
type Stats struct {
	Policy     Policy        `json:"policy"`
	QueueSize  int           `json:"queue_size"`
	Published  int64         `json:"published"`
	HeapBytes  uint64        `json:"heap_bytes"`
	Goroutines int           `json:"goroutines"`
	Clients    []ClientStats `json:"clients"`
}

type ClientStats struct {
	ID        uint64 `json:"id"`
	Remote    string `json:"remote"` // matches the client's local address, so it can find itself
	Topics    int    `json:"topics"`
	Queued    int    `json:"queued"`
	Delivered int64  `json:"delivered"`
	Dropped   int64  `json:"dropped"`
}

func (b *Broker) Stats() Stats {
	// ReadMemStats briefly pauses the program. Fine once a second for a
	// demo; you wouldn't call it on every request in production.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	s := Stats{
		Policy:     b.cfg.Policy,
		QueueSize:  b.cfg.QueueSize,
		Published:  b.published.Load(),
		HeapBytes:  mem.HeapAlloc,
		Goroutines: runtime.NumGoroutine(),
	}

	b.mu.RLock()
	for c := range b.clients {
		cs := ClientStats{
			ID:        c.id,
			Remote:    c.conn.RemoteAddr().String(),
			Topics:    len(c.topics),
			Queued:    c.out.len(),
			Delivered: c.delivered.Load(),
			Dropped:   c.dropped.Load(),
		}
		s.Clients = append(s.Clients, cs)
	}
	b.mu.RUnlock()

	slices.SortFunc(s.Clients, func(a, b ClientStats) int { return cmp.Compare(a.ID, b.ID) })
	return s
}

// StatsHandler serves Stats as JSON.
func (b *Broker) StatsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(b.Stats()); err != nil {
			b.log.Warn("encode stats failed", "err", err)
		}
	})
}
