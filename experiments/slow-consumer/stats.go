package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/avii09/go_pub_sub/broker"
)

var statsClient = &http.Client{Timeout: 500 * time.Millisecond}

// fetchStats asks the broker what it looks like from the inside: its
// policy, memory, and how full each subscriber's queue is.
func fetchStats(url string) (broker.Stats, error) {
	var s broker.Stats
	resp, err := statsClient.Get(url)
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s, fmt.Errorf("stats: HTTP %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&s)
	return s, err
}

// clientByAddr indexes the broker's client list by remote address, which
// is the same as each of our connections' local address.
func clientByAddr(s broker.Stats) map[string]broker.ClientStats {
	m := make(map[string]broker.ClientStats, len(s.Clients))
	for _, c := range s.Clients {
		m[c.Remote] = c
	}
	return m
}
