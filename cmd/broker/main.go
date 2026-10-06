// Command broker runs the pub/sub broker over TCP.
//
//	go run ./cmd/broker -policy drop
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avii09/go_pub_sub/broker"
)

func main() {
	addr := flag.String("addr", ":7777", "address to listen on for pub/sub clients")
	statsAddr := flag.String("stats-addr", ":7778", "address for the HTTP /stats endpoint (empty to disable)")
	policyName := flag.String("policy", string(broker.PolicyBlock), "what to do when a subscriber can't keep up: drop, buffer or block")
	queueSize := flag.Int("queue", broker.DefaultQueueSize, "per-subscriber queue length (drop and block)")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	policy, err := broker.ParsePolicy(*policyName)
	if err != nil {
		log.Error("bad -policy flag", "err", err)
		os.Exit(2)
	}

	// Ctrl+C cancels ctx, which makes Serve shut down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen failed", "addr", *addr, "err", err)
		os.Exit(1)
	}

	b := broker.New(log, broker.Config{Policy: policy, QueueSize: *queueSize})

	if *statsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/stats", b.StatsHandler())
		srv := &http.Server{Addr: *statsAddr, Handler: mux}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("stats server failed", "addr", *statsAddr, "err", err)
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			srv.Shutdown(shutdownCtx)
		}()
	}

	log.Info("broker listening", "addr", ln.Addr().String(), "policy", policy, "queue", *queueSize, "stats", *statsAddr)
	if err := b.Serve(ctx, ln); err != nil {
		log.Error("serve failed", "err", err)
		os.Exit(1)
	}
	log.Info("broker stopped")
}
