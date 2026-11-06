// Command bench measures offset-consumer throughput under different commit
// scenarios. It consumes -msgs messages from -stream with a fresh consumer
// name and prints one JSON line with the results.
//
//	go run ./bench -scenario sync-100 -msgs 2000000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	oc "github.com/synadia-io/offset-consumer-go"
)

type result struct {
	Lang      string  `json:"lang"`
	Scenario  string  `json:"scenario"`
	Msgs      int64   `json:"msgs"`
	Bytes     int64   `json:"bytes"`
	RecvMs    float64 `json:"recv_ms"`
	ElapsedMs float64 `json:"elapsed_ms"`
	Commits   int64   `json:"commits"`
	FinalSeq  uint64  `json:"final_seq"`
}

func main() {
	url := flag.String("server", nats.DefaultURL, "server URL")
	stream := flag.String("stream", "BENCH", "stream name")
	msgs := flag.Int64("msgs", 2_000_000, "messages to consume")
	scenario := flag.String("scenario", "none", "none | auto-1s | async-N | sync-N")
	flag.Parse()

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	cfg := oc.ConsumerConfig{
		Stream:       *stream,
		ConsumerName: fmt.Sprintf("bench-go-%s-%d", *scenario, time.Now().UnixNano()),
	}
	mode, every := parseScenario(*scenario)
	if mode == "auto" {
		cfg.AutoCommit = true
		cfg.AutoCommitInterval = time.Second
	}

	c, err := oc.NewConsumer(ctx, js, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer c.DeleteConsumer(ctx)

	var count, bytes, commits atomic.Int64
	done := make(chan struct{})
	var doneOnce atomic.Bool
	var lastFuture atomic.Pointer[jetstream.PubAckFuture]
	var recvNs atomic.Int64

	start := time.Now()
	err = c.Consume(func(msg jetstream.Msg) {
		n := count.Add(1)
		if n > *msgs {
			return
		}
		bytes.Add(int64(len(msg.Data())))
		switch mode {
		case "sync":
			if n%every == 0 {
				if err := c.CommitSync(ctx); err != nil {
					log.Fatalf("CommitSync: %v", err)
				}
				commits.Add(1)
			}
		case "async":
			if n%every == 0 {
				f, err := c.CommitAsync()
				if err != nil {
					log.Fatalf("CommitAsync: %v", err)
				}
				lastFuture.Store(&f)
				commits.Add(1)
			}
		}
		if n == *msgs && doneOnce.CompareAndSwap(false, true) {
			recvNs.Store(int64(time.Since(start)))
			close(done)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	<-done

	// Settle: wait for the last async future, then commit the final position
	// synchronously so every scenario ends with the offset persisted.
	if f := lastFuture.Load(); f != nil {
		select {
		case <-(*f).Ok():
		case err := <-(*f).Err():
			log.Fatalf("async commit failed: %v", err)
		}
	}
	if mode != "auto" {
		if err := c.CommitSync(ctx); err != nil {
			log.Fatalf("final CommitSync: %v", err)
		}
		commits.Add(1)
	}
	if err := c.Close(); err != nil {
		log.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)

	info, err := c.Info(ctx)
	if err != nil || info == nil {
		log.Fatalf("Info: %v", err)
	}

	out, _ := json.Marshal(result{
		Lang:      "go",
		Scenario:  *scenario,
		Msgs:      *msgs,
		Bytes:     bytes.Load(),
		RecvMs:    float64(recvNs.Load()) / 1e6,
		ElapsedMs: float64(elapsed.Microseconds()) / 1000,
		Commits:   commits.Load(),
		FinalSeq:  info.Seq,
	})
	fmt.Println(string(out))
	if info.Seq < uint64(*msgs) {
		fmt.Fprintf(os.Stderr, "warning: final seq %d < msgs %d\n", info.Seq, *msgs)
	}
}

func parseScenario(s string) (mode string, every int64) {
	switch {
	case s == "none":
		return "none", 0
	case strings.HasPrefix(s, "auto"):
		return "auto", 0
	case strings.HasPrefix(s, "async-"):
		n, _ := strconv.ParseInt(strings.TrimPrefix(s, "async-"), 10, 64)
		return "async", n
	case strings.HasPrefix(s, "sync-"):
		n, _ := strconv.ParseInt(strings.TrimPrefix(s, "sync-"), 10, 64)
		return "sync", n
	}
	log.Fatalf("unknown scenario %q", s)
	return "", 0
}
