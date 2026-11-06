package offsetconsumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const testStream = "test-stream"
const numMessages = 100

// setupStream creates a stream and publishes numMessages messages.
// Returns a connected JetStream context.
func setupStream(t *testing.T, serverURL string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()

	// Create stream
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     testStream,
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Publish messages
	for i := 1; i <= numMessages; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("test.%d", i), []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("publish msg %d: %v", i, err)
		}
	}

	return js
}

// connect creates a new JetStream connection to the server (for reconnect tests)
func connect(t *testing.T, serverURL string) jetstream.JetStream {
	t.Helper()
	js, _ := connectRaw(t, serverURL)
	return js
}

// connectRaw creates a new connection and returns both the JetStream context
// and the underlying connection so tests can close it abruptly.
func connectRaw(t *testing.T, serverURL string) (jetstream.JetStream, *nats.Conn) {
	t.Helper()
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js, nc
}

// waitUntil polls cond until it returns true or timeout elapses.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

// lastDelivered returns the consumer's last delivered sequence.
func lastDelivered(c OffsetConsumer) uint64 {
	impl := c.(*offsetConsumer)
	impl.mu.Lock()
	defer impl.mu.Unlock()
	return impl.lastDeliveredSeq
}

// consumeUntilDelivered starts consumption and waits until at least one
// message has been delivered.
func consumeUntilDelivered(t *testing.T, c OffsetConsumer) {
	t.Helper()
	if err := c.Consume(func(msg jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c) > 0 }) {
		t.Fatal("no messages delivered")
	}
}

// consumerNamesWithPrefix lists the stream's consumers whose name starts with prefix.
func consumerNamesWithPrefix(t *testing.T, js jetstream.JetStream, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	stream, err := js.Stream(ctx, testStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var names []string
	lister := stream.ConsumerNames(ctx)
	for name := range lister.Name() {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("consumer names: %v", err)
	}
	return names
}

func awaitFuture(t *testing.T, f jetstream.PubAckFuture, what string) {
	t.Helper()
	if f == nil {
		t.Fatalf("%s: nil future", what)
	}
	select {
	case <-f.Ok():
	case err := <-f.Err():
		t.Fatalf("%s failed: %v", what, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("%s timed out", what)
	}
}

// Test 1: Basic consume — create consumer, receive messages, verify sequence tracking
func TestBasicConsume(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-basic-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close()

	var count atomic.Int64
	err = c.Consume(func(msg jetstream.Msg) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	waitUntil(5*time.Second, func() bool { return count.Load() >= numMessages })

	n := count.Load()
	if n == 0 {
		t.Fatal("expected to receive messages, got 0")
	}
	t.Logf("Received %d messages", n)
}

// Test 2: CommitSync and resume — commit, close, reopen, verify resume position
func TestCommitSyncAndResume(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-resume-%d", time.Now().UnixNano())

	ctx := context.Background()

	// Phase 1: consume some messages and commit
	c1, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer (phase 1): %v", err)
	}

	var lastSeq atomic.Uint64
	err = c1.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		if md != nil {
			lastSeq.Store(md.Sequence.Stream)
		}
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	waitUntil(5*time.Second, func() bool { return lastSeq.Load() >= 10 })

	committedSeq := lastSeq.Load()
	if committedSeq == 0 {
		t.Fatal("no messages received")
	}

	err = c1.CommitSync(ctx)
	if err != nil {
		t.Fatalf("CommitSync: %v", err)
	}
	t.Logf("Phase 1: committed at seq %d", committedSeq)

	c1.Close()

	// Publish more messages so the resumed consumer has something to consume
	for i := numMessages + 1; i <= numMessages+10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("test.%d", i), []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("publish msg %d: %v", i, err)
		}
	}

	// Phase 2: reopen and verify resume from committed position
	js2 := connect(t, srv.ClientURL())
	c2, err := NewConsumer(ctx, js2, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer (phase 2): %v", err)
	}
	defer c2.Close()

	var firstResumeSeq atomic.Uint64
	var gotFirst atomic.Bool
	err = c2.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		if md != nil && !gotFirst.Load() {
			gotFirst.Store(true)
			firstResumeSeq.Store(md.Sequence.Stream)
		}
	})
	if err != nil {
		t.Fatalf("Consume (phase 2): %v", err)
	}

	waitUntil(5*time.Second, func() bool { return gotFirst.Load() })

	resumeSeq := firstResumeSeq.Load()
	t.Logf("Phase 2: first message after resume at seq %d (expected %d)", resumeSeq, committedSeq+1)

	if resumeSeq != committedSeq+1 {
		t.Errorf("expected resume at seq %d, got %d", committedSeq+1, resumeSeq)
	}

	c2.DeleteConsumer(ctx)
}

// Test 3: CommitAsync — the future completes and the committed offset is
// only recorded once the server has acknowledged the write.
func TestCommitAsync(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-async-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	consumeUntilDelivered(t, c)
	waitUntil(5*time.Second, func() bool { return lastDelivered(c) >= numMessages })
	delivered := lastDelivered(c)

	f, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("CommitAsync: %v", err)
	}
	awaitFuture(t, f, "CommitAsync")

	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info == nil || info.Seq != delivered {
		t.Fatalf("expected stored seq %d after ack, got %+v", delivered, info)
	}
	impl := c.(*offsetConsumer)
	impl.mu.Lock()
	committed := impl.lastCommittedSeq
	impl.mu.Unlock()
	if committed != delivered {
		t.Fatalf("expected lastCommittedSeq %d after ack, got %d", delivered, committed)
	}
}

// Test 4: several async commits may be in flight at once; all complete.
func TestCommitAsyncChained(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-chained-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	consumeUntilDelivered(t, c)

	f1, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("first CommitAsync: %v", err)
	}
	f2, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("second CommitAsync: %v", err)
	}
	f3, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("third CommitAsync: %v", err)
	}
	awaitFuture(t, f1, "first commit")
	awaitFuture(t, f2, "second commit")
	awaitFuture(t, f3, "third commit")
	t.Log("three pipelined async commits acknowledged")
}

// Test 5: CommitAsync with nothing to commit returns a completed future.
func TestCommitAsyncNothingToCommit(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-nothing-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	f, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("CommitAsync: %v", err)
	}
	awaitFuture(t, f, "empty commit")
	if err := c.CommitSync(ctx); err != nil {
		t.Fatalf("CommitSync with nothing to commit: %v", err)
	}
}

// Test 6: AutoCommit mode
func TestAutoCommit(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-autocommit-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:             testStream,
		ConsumerName:       consumerName,
		AutoCommit:         true,
		AutoCommitInterval: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	var count atomic.Int64
	err = c.Consume(func(msg jetstream.Msg) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	waitUntil(5*time.Second, func() bool { return count.Load() >= numMessages })

	// Wait for auto-commit to fire at least once
	time.Sleep(2 * time.Second)

	n := count.Load()
	t.Logf("Received %d messages before close", n)

	err = c.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify offset was committed by reopening
	js2 := connect(t, srv.ClientURL())
	c2, err := NewConsumer(ctx, js2, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer (verify): %v", err)
	}
	impl := c2.(*offsetConsumer)
	t.Logf("Stored offset after auto-commit: %d", impl.startSeq)
	if impl.startSeq == 0 {
		t.Error("expected non-zero stored offset after auto-commit")
	}
	c2.Close()
	c2.DeleteConsumer(ctx)
}

// Test 7: Seek
func TestSeek(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-seek-%d", time.Now().UnixNano())

	ctx := context.Background()

	// Phase 1: create consumer, consume some messages, commit, close
	c1, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	consumeUntilDelivered(t, c1)
	c1.CommitSync(ctx)
	c1.Close()

	// Phase 2: reopen, seek to 50, then consume and check first message
	js2 := connect(t, srv.ClientURL())
	c2, err := NewConsumer(ctx, js2, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer (phase 2): %v", err)
	}
	defer func() {
		c2.Close()
		c2.DeleteConsumer(ctx)
	}()

	err = c2.Seek(ctx, 50)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}

	var firstSeq atomic.Uint64
	var gotFirst atomic.Bool
	err = c2.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		if md != nil && !gotFirst.Load() {
			gotFirst.Store(true)
			firstSeq.Store(md.Sequence.Stream)
		}
	})
	if err != nil {
		t.Fatalf("Consume after seek: %v", err)
	}

	waitUntil(5*time.Second, func() bool { return gotFirst.Load() })

	seq := firstSeq.Load()
	t.Logf("First message after Seek(50): seq %d (expected 51)", seq)
	if seq != 51 {
		t.Errorf("expected seq 51 after Seek(50), got %d", seq)
	}
}

// Test 8: Seek while consuming restarts delivery under a new generation
// prefix and removes the previous ordered consumer.
func TestSeekRestartsWithNewGeneration(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-seekgen-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	var firstAfterSeek atomic.Uint64
	var seeked atomic.Bool
	err = c.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		if md != nil && seeked.Load() && firstAfterSeek.Load() == 0 {
			firstAfterSeek.Store(md.Sequence.Stream)
		}
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	waitUntil(5*time.Second, func() bool { return lastDelivered(c) >= numMessages })

	impl := c.(*offsetConsumer)
	before := consumerNamesWithPrefix(t, js, impl.owner)
	if len(before) != 1 || !strings.Contains(before[0], "-g1_") {
		t.Fatalf("expected one generation-1 consumer, got %v", before)
	}

	seeked.Store(true)
	if err := c.Seek(ctx, 50); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	waitUntil(5*time.Second, func() bool { return firstAfterSeek.Load() != 0 })
	if got := firstAfterSeek.Load(); got != 51 {
		t.Errorf("expected first message after Seek(50) to be 51, got %d", got)
	}

	after := consumerNamesWithPrefix(t, js, impl.owner)
	if len(after) != 1 || !strings.Contains(after[0], "-g2_") {
		t.Errorf("expected exactly one generation-2 consumer after seek, got %v", after)
	}
}

// Test 9: SeekToBeginning
func TestSeekToBeginning(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-seekbegin-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	consumeUntilDelivered(t, c)
	c.CommitSync(ctx)

	err = c.SeekToBeginning(ctx)
	if err != nil {
		t.Fatalf("SeekToBeginning: %v", err)
	}

	impl := c.(*offsetConsumer)
	impl.mu.Lock()
	s := impl.startSeq
	impl.mu.Unlock()
	t.Logf("startSeq after SeekToBeginning: %d (expected 0)", s)
	if s != 0 {
		t.Errorf("expected startSeq 0 after SeekToBeginning, got %d", s)
	}
}

// Test 10: DeleteConsumer
func TestDeleteConsumer(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-delete-%d", time.Now().UnixNano())

	ctx := context.Background()

	// Create and commit an offset
	c1, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	consumeUntilDelivered(t, c1)
	c1.CommitSync(ctx)
	c1.Close()

	// Delete the consumer offset
	js2 := connect(t, srv.ClientURL())
	c2, err := NewConsumer(ctx, js2, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer (for delete): %v", err)
	}
	impl2 := c2.(*offsetConsumer)
	if impl2.startSeq == 0 {
		t.Fatal("expected non-zero startSeq before delete")
	}
	t.Logf("Before delete: startSeq=%d", impl2.startSeq)

	err = c2.DeleteConsumer(ctx)
	if err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}
	c2.Close()

	// Close after DeleteConsumer must not recreate the entry
	if info, err := c2.Info(ctx); err != nil || info != nil {
		t.Fatalf("expected no entry after delete+close, got info=%+v err=%v", info, err)
	}

	// New consumer with same name should start from beginning
	js3 := connect(t, srv.ClientURL())
	c3, err := NewConsumer(ctx, js3, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer (after delete): %v", err)
	}
	impl3 := c3.(*offsetConsumer)
	t.Logf("After delete: startSeq=%d (expected 0)", impl3.startSeq)
	if impl3.startSeq != 0 {
		t.Errorf("expected startSeq 0 after delete, got %d", impl3.startSeq)
	}
	c3.Close()
}

// Test 11: Invalid consumer name
func TestInvalidConsumerName(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	// Empty name
	_, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: "",
	})
	if err != ErrInvalidConsumerName {
		t.Errorf("expected ErrInvalidConsumerName for empty name, got %v", err)
	}

	// Name with dot
	_, err = NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: "has.dot",
	})
	if err != ErrInvalidConsumerName {
		t.Errorf("expected ErrInvalidConsumerName for dotted name, got %v", err)
	}

	t.Log("Invalid consumer name validation works correctly")
}

// Test 12: StartPosition End
func TestStartPositionEnd(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-startend-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:        testStream,
		ConsumerName:  consumerName,
		StartPosition: StartPositionEnd,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	var count atomic.Int64
	err = c.Consume(func(msg jetstream.Msg) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// With StartPositionEnd and no new publishes, should get 0 messages
	time.Sleep(2 * time.Second)
	n := count.Load()
	t.Logf("Messages received with StartPositionEnd (no new publishes): %d", n)
	if n > 0 {
		t.Errorf("expected 0 messages with StartPositionEnd (no new publishes), got %d", n)
	}
}

// Test 13: ErrAlreadyConsuming
func TestAlreadyConsuming(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-already-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	err = c.Consume(func(msg jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Second call should fail
	err = c.Consume(func(msg jetstream.Msg) {})
	if err != ErrAlreadyConsuming {
		t.Errorf("expected ErrAlreadyConsuming, got %v", err)
	}
	t.Log("ErrAlreadyConsuming works correctly")
}

// Test 14: Info and ListConsumers expose the ownership fields.
func TestInfoAndListHaveOwner(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-owner-%d", time.Now().UnixNano())

	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c.Close()
		c.DeleteConsumer(ctx)
	}()

	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info == nil {
		t.Fatal("expected an entry right after NewConsumer (claim)")
	}
	wantPrefix := "oc-" + testStream + "-" + consumerName + "-"
	if !strings.HasPrefix(info.Owner, wantPrefix) {
		t.Errorf("owner %q does not start with %q", info.Owner, wantPrefix)
	}
	host, _ := os.Hostname()
	if info.Host != host {
		t.Errorf("host %q, want %q", info.Host, host)
	}
	if _, err := time.Parse(time.RFC3339, info.ClaimedAt); err != nil {
		t.Errorf("claimed_at %q is not RFC3339: %v", info.ClaimedAt, err)
	}
	if info.Seq != 0 {
		t.Errorf("expected seq 0 before any commit, got %d", info.Seq)
	}

	consumeUntilDelivered(t, c)
	if err := c.CommitSync(ctx); err != nil {
		t.Fatalf("CommitSync: %v", err)
	}

	list, err := ListConsumers(ctx, js, testStream)
	if err != nil {
		t.Fatalf("ListConsumers: %v", err)
	}
	got, ok := list[consumerName]
	if !ok {
		t.Fatalf("ListConsumers missing %q: %v", consumerName, list)
	}
	if got.Owner != info.Owner || got.Seq == 0 {
		t.Errorf("unexpected list entry %+v", got)
	}

	// The commit must have kept the ownership fields.
	after, _ := c.Info(ctx)
	if after.Owner != info.Owner || after.ClaimedAt != info.ClaimedAt {
		t.Errorf("commit changed ownership fields: before %+v after %+v", info, after)
	}
}

// Test 15: a second instance for the same stream/consumer is refused while
// the first one is alive.
func TestSecondInstanceRefused(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-refused-%d", time.Now().UnixNano())

	ctx := context.Background()
	c1, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() {
		c1.Close()
		c1.DeleteConsumer(ctx)
	}()
	consumeUntilDelivered(t, c1)
	info, _ := c1.Info(ctx)

	js2 := connect(t, srv.ClientURL())
	c2, err := NewConsumer(ctx, js2, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err == nil {
		c2.Close()
		t.Fatal("expected second instance to be refused")
	}
	if !errors.Is(err, ErrAlreadyInUse) {
		t.Fatalf("expected ErrAlreadyInUse, got %v", err)
	}
	var inUse *AlreadyInUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("expected *AlreadyInUseError, got %T", err)
	}
	if inUse.Owner != info.Owner || inUse.Host != info.Host || inUse.ClaimedAt != info.ClaimedAt {
		t.Errorf("error details %+v do not match entry %+v", inUse, info)
	}
	t.Logf("refused: %v", err)

	// The refused attempt must not have disturbed the owner's entry.
	again, _ := c1.Info(ctx)
	if again.Owner != info.Owner {
		t.Errorf("owner changed after refused attempt: %q -> %q", info.Owner, again.Owner)
	}
}

// Test 16: Close deletes the ordered consumer and releases ownership so a
// new instance can start immediately.
func TestReleaseOnClose(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-release-%d", time.Now().UnixNano())

	ctx := context.Background()
	c1, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumeUntilDelivered(t, c1)
	if err := c1.CommitSync(ctx); err != nil {
		t.Fatalf("CommitSync: %v", err)
	}
	owner1 := c1.(*offsetConsumer).owner
	committed := lastDelivered(c1)

	if names := consumerNamesWithPrefix(t, js, owner1); len(names) != 1 {
		t.Fatalf("expected one live ordered consumer before close, got %v", names)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if names := consumerNamesWithPrefix(t, js, owner1); len(names) != 0 {
		t.Errorf("expected ordered consumer deleted on close, got %v", names)
	}

	info, err := c1.Info(ctx)
	if err != nil || info == nil {
		t.Fatalf("Info after close: info=%+v err=%v", info, err)
	}
	if info.Owner != "" || info.Host != "" || info.ClaimedAt != "" {
		t.Errorf("expected ownership released on close, got %+v", info)
	}
	if info.Seq != committed {
		t.Errorf("close must keep the committed seq %d (AutoCommit off), got %d", committed, info.Seq)
	}

	js2 := connect(t, srv.ClientURL())
	c2, err := NewConsumer(ctx, js2, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("expected immediate restart after close, got %v", err)
	}
	defer func() {
		c2.Close()
		c2.DeleteConsumer(ctx)
	}()
	info2, _ := c2.Info(ctx)
	if info2.Owner == "" || info2.Owner == owner1 {
		t.Errorf("expected a fresh owner, got %q", info2.Owner)
	}
	if c2.(*offsetConsumer).startSeq != committed {
		t.Errorf("expected startSeq %d, got %d", committed, c2.(*offsetConsumer).startSeq)
	}
}

// Test 17: after a crash (connection dropped without Close) a new instance is
// refused until the ordered consumer expires, then allowed.
func TestCrashRecovery(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-crash-%d", time.Now().UnixNano())
	ctx := context.Background()

	js1, nc1 := connectRaw(t, srv.ClientURL())
	c1, err := NewConsumer(ctx, js1, ConsumerConfig{
		Stream:            testStream,
		ConsumerName:      consumerName,
		InactiveThreshold: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumeUntilDelivered(t, c1)
	owner1 := c1.(*offsetConsumer).owner

	// Simulate a crash: drop the connection without calling Close.
	nc1.Close()

	cfg := ConsumerConfig{Stream: testStream, ConsumerName: consumerName, InactiveThreshold: 2 * time.Second}
	_, err = NewConsumer(ctx, js, cfg)
	if !errors.Is(err, ErrAlreadyInUse) {
		t.Fatalf("expected refusal right after crash, got %v", err)
	}
	t.Logf("right after crash: %v", err)

	start := time.Now()
	var c2 OffsetConsumer
	ok := waitUntil(15*time.Second, func() bool {
		c, err := NewConsumer(ctx, js, cfg)
		if err == nil {
			c2 = c
			return true
		}
		if !errors.Is(err, ErrAlreadyInUse) {
			t.Fatalf("unexpected error while waiting for recovery: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		return false
	})
	if !ok {
		t.Fatal("new instance never allowed after crash")
	}
	defer func() {
		c2.Close()
		c2.DeleteConsumer(ctx)
	}()
	t.Logf("recovered after %v", time.Since(start).Round(100*time.Millisecond))
	if names := consumerNamesWithPrefix(t, js, owner1); len(names) != 0 {
		t.Errorf("crashed owner's consumer still present: %v", names)
	}
	if info, _ := c2.Info(ctx); info == nil || info.Owner == owner1 {
		t.Errorf("expected a new owner after recovery, got %+v", info)
	}
}

// Test 18: the bucket is created with a 10 minute delete-marker TTL.
func TestBucketDeleteMarkerTTL(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: fmt.Sprintf("test-ttl-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close()

	stream, err := js.Stream(ctx, "KV_"+kvBucketName)
	if err != nil {
		t.Fatalf("bucket stream: %v", err)
	}
	cfg := stream.CachedInfo().Config
	if cfg.SubjectDeleteMarkerTTL != 10*time.Minute {
		t.Errorf("expected a 10m delete-marker TTL, got %v", cfg.SubjectDeleteMarkerTTL)
	}
	if !cfg.AllowMsgTTL {
		t.Error("expected per-message TTL to be enabled on the bucket stream")
	}
}

// Test 19: Close with async commits still in flight persists the highest
// requested position and releases ownership, even when the bounded wait expires.
func TestCloseWithCommitBacklog(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	c, err := NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: fmt.Sprintf("test-backlog-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.DeleteConsumer(ctx)

	consumeUntilDelivered(t, c)
	waitUntil(5*time.Second, func() bool { return lastDelivered(c) >= numMessages })
	delivered := lastDelivered(c)
	owner := c.(*offsetConsumer).owner

	// Flood the commit pipeline; stop when the client's async window is full.
	var last jetstream.PubAckFuture
	for i := 0; i < 20000; i++ {
		f, err := c.CommitAsync()
		if err != nil {
			break
		}
		last = f
	}

	// Make the bounded wait expire almost immediately.
	old := closeTimeout
	closeTimeout = time.Millisecond
	defer func() { closeTimeout = old }()

	start := time.Now()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("Close took %v", took)
	}
	if last != nil {
		awaitFuture(t, last, "last async commit")
	}

	info, err := c.Info(ctx)
	if err != nil || info == nil {
		t.Fatalf("Info: %+v %v", info, err)
	}
	if info.Seq != delivered {
		t.Errorf("expected the release write to persist seq %d, got %d", delivered, info.Seq)
	}
	if info.Owner != "" {
		t.Errorf("expected ownership released, got owner %q", info.Owner)
	}
	if names := consumerNamesWithPrefix(t, js, owner); len(names) != 0 {
		t.Errorf("ordered consumer still present: %v", names)
	}
}
