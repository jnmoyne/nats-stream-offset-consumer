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
	stored := storedSeq(t, c2)
	t.Logf("Stored offset after auto-commit: %d", stored)
	if stored == 0 {
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
	before := consumerNamesWithPrefix(t, js, impl.owner+"-g")
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

	after := consumerNamesWithPrefix(t, js, impl.owner+"-g")
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
	if storedSeq(t, c2) == 0 {
		t.Fatal("expected a non-zero stored offset before delete")
	}
	t.Logf("Before delete: stored=%d", storedSeq(t, c2))

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
	if !errors.Is(err, ErrInvalidConsumerName) {
		t.Errorf("expected ErrInvalidConsumerName for empty name, got %v", err)
	}

	// Name with dot
	_, err = NewConsumer(ctx, js, ConsumerConfig{
		Stream:       testStream,
		ConsumerName: "has.dot",
	})
	if !errors.Is(err, ErrInvalidConsumerName) {
		t.Errorf("expected ErrInvalidConsumerName for dotted name, got %v", err)
	}

	for _, name := range []string{"has*star", "has>gt", "has space", "has/slash", strings.Repeat("x", 200)} {
		_, err = NewConsumer(ctx, js, ConsumerConfig{Stream: testStream, ConsumerName: name})
		if !errors.Is(err, ErrInvalidConsumerName) {
			t.Errorf("expected ErrInvalidConsumerName for %q, got %v", name, err)
		}
	}
	_, err = NewConsumer(ctx, js, ConsumerConfig{Stream: "bad stream", ConsumerName: "ok"})
	if !errors.Is(err, ErrInvalidStreamName) {
		t.Errorf("expected ErrInvalidStreamName, got %v", err)
	}
	if _, err = NewConsumer(ctx, js, ConsumerConfig{Stream: testStream, ConsumerName: "Ok_name-1=x"}); err != nil {
		t.Errorf("valid name rejected: %v", err)
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

	if info, err := c.Info(ctx); err != nil || info != nil {
		t.Fatalf("expected no entry before Consume (no claim yet), got %+v (%v)", info, err)
	}
	if err := c.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info == nil {
		t.Fatal("expected an entry right after Consume (claim)")
	}
	wantPrefix := "oc-" + testStream + "-" + consumerName + "-"
	if !strings.HasPrefix(info.Owner, wantPrefix) {
		t.Errorf("owner %q does not start with %q", info.Owner, wantPrefix)
	}
	host, _ := os.Hostname()
	if info.Host != host {
		t.Errorf("host %q, want %q", info.Host, host)
	}
	if info.Seq != 0 {
		t.Errorf("expected seq 0 before any commit, got %d", info.Seq)
	}

	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c) > 0 }) {
		t.Fatal("no messages delivered")
	}
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
	if after.Owner != info.Owner || after.Host != info.Host {
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
	if inUse.Owner != info.Owner || inUse.Host != info.Host {
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

	if names := consumerNamesWithPrefix(t, js, owner1+"-g"); len(names) != 1 {
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
	if info.Owner != "" || info.Host != "" {
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
	if err := c2.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume after restart: %v", err)
	}
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
	if names := consumerNamesWithPrefix(t, js, owner1); len(names) != 0 {
		t.Errorf("crashed owner's consumer still present: %v", names)
	}
	// The claim at Consume waits livenessWait before taking over.
	if err := c2.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume after recovery: %v", err)
	}
	t.Logf("recovered after %v", time.Since(start).Round(100*time.Millisecond))
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

// newTestConsumer creates a consumer with a unique name and registers cleanup.
func newTestConsumer(t *testing.T, js jetstream.JetStream, cfg ConsumerConfig) OffsetConsumer {
	t.Helper()
	if cfg.Stream == "" {
		cfg.Stream = testStream
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = fmt.Sprintf("c-%d", time.Now().UnixNano())
	}
	c, err := NewConsumer(context.Background(), js, cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func storedSeq(t *testing.T, c OffsetConsumer) uint64 {
	t.Helper()
	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info == nil {
		return 0
	}
	return info.Seq
}

// Auto-commit must not commit a message whose handler is still running.
func TestAutoCommitSkipsMessageInHandler(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())

	c := newTestConsumer(t, js, ConsumerConfig{AutoCommit: true, AutoCommitInterval: 100 * time.Millisecond})
	release := make(chan struct{})
	var handled atomic.Int64
	if err := c.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		if md.Sequence.Stream == 3 {
			<-release
		}
		handled.Store(int64(md.Sequence.Stream))
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if !waitUntil(5*time.Second, func() bool { return storedSeq(t, c) == 2 }) {
		t.Fatalf("expected stored seq 2 while message 3 is in its handler, got %d", storedSeq(t, c))
	}
	time.Sleep(500 * time.Millisecond)
	if got := storedSeq(t, c); got != 2 {
		t.Fatalf("auto-commit committed a message still in its handler: stored %d", got)
	}
	close(release)
	if !waitUntil(5*time.Second, func() bool { return storedSeq(t, c) == numMessages }) {
		t.Fatalf("expected all messages committed after release, got %d", storedSeq(t, c))
	}
}

// Close with auto-commit must not commit a message whose handler is still running.
func TestCloseAutoCommitSkipsMessageInHandler(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	c, err := NewConsumer(context.Background(), js, ConsumerConfig{Stream: testStream, ConsumerName: name, AutoCommit: true, AutoCommitInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	if err := c.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		if md.Sequence.Stream == 5 {
			close(entered)
			<-release
		}
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	<-entered
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(release)

	c2 := newTestConsumer(t, connect(t, srv.ClientURL()), ConsumerConfig{ConsumerName: name})
	if got := storedSeq(t, c2); got != 4 {
		t.Fatalf("expected stored seq 4 (message 5 was still in its handler), got %d", got)
	}
}

// StartPositionEnd is resolved at creation and stored by the claim, so an end
// consumer that received nothing before closing resumes there: messages
// published while it is down are delivered on the next start.
func TestStartPositionEndPersisted(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	c, err := NewConsumer(ctx, js, ConsumerConfig{Stream: testStream, ConsumerName: name, StartPosition: StartPositionEnd})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	for i := 0; i < 2; i++ { // published after creation, before Consume
		if _, err := js.Publish(ctx, "test.early", []byte("early")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c) == numMessages+2 }) {
		t.Fatalf("expected the messages published after creation, last delivered %d", lastDelivered(c))
	}
	if err := c.CommitSync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := js.Publish(ctx, "test.new", []byte("new")); err != nil {
			t.Fatal(err)
		}
	}

	c2 := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name, StartPosition: StartPositionEnd})
	var first atomic.Uint64
	if err := c2.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		first.CompareAndSwap(0, md.Sequence.Stream)
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c2) == numMessages+5 }) {
		t.Fatalf("expected messages published while down, last delivered %d", lastDelivered(c2))
	}
	if first.Load() != numMessages+3 {
		t.Fatalf("expected first message %d, got %d", numMessages+3, first.Load())
	}
}

// SeekToBeginning goes to the beginning even with StartPositionEnd.
func TestSeekToBeginningWithStartPositionEnd(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())

	c := newTestConsumer(t, js, ConsumerConfig{StartPosition: StartPositionEnd})
	var first atomic.Uint64
	if err := c.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		first.CompareAndSwap(0, md.Sequence.Stream)
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := c.SeekToBeginning(context.Background()); err != nil {
		t.Fatalf("SeekToBeginning: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c) == numMessages }) {
		t.Fatalf("expected all messages after SeekToBeginning, last delivered %d", lastDelivered(c))
	}
	if first.Load() != 1 {
		t.Fatalf("expected first message 1, got %d", first.Load())
	}
}

// An existing bucket's configuration is not overwritten.
func TestExistingBucketNotOverwritten(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: kvBucketName, Description: "operator managed", History: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	newTestConsumer(t, js, ConsumerConfig{})
	kv, err := js.KeyValue(ctx, kvBucketName)
	if err != nil {
		t.Fatal(err)
	}
	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := status.(*jetstream.KeyValueBucketStatus).StreamInfo().Config
	if cfg.Description != "operator managed" {
		t.Fatalf("bucket configuration was overwritten: description %q", cfg.Description)
	}
}

// Concurrent seeks leave exactly one ordered consumer.
func TestConcurrentSeeks(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	consumeUntilDelivered(t, c)
	owner := c.(*offsetConsumer).owner

	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(i int) { done <- c.Seek(ctx, uint64(i*5)) }(i)
	}
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatalf("Seek: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if names := consumerNamesWithPrefix(t, js, owner+"-g"); len(names) != 1 {
		t.Fatalf("expected exactly one ordered consumer, got %v", names)
	}
}

// The ErrorHandler may call Close when an auto-commit fails.
func TestErrorHandlerCanCloseOnAutoCommitFailure(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	closed := make(chan error, 1)
	var once atomic.Bool
	c := newTestConsumer(t, js, ConsumerConfig{
		AutoCommit:         true,
		AutoCommitInterval: 200 * time.Millisecond,
		ErrorHandler: func(oc OffsetConsumer, err error) {
			if strings.Contains(err.Error(), "auto-commit failed") && once.CompareAndSwap(false, true) {
				closed <- oc.Close()
			}
		},
	})
	if err := c.Consume(func(msg jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	// Make commits fail: without the bucket, KV writes get no responders.
	if err := js.DeleteKeyValue(ctx, kvBucketName); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close called from the ErrorHandler did not return (deadlock)")
	}
}

// A commit whose ack never arrives fails instead of hanging forever.
func TestCommitAckTimeout(t *testing.T) {
	old := commitAckTimeout
	commitAckTimeout = 300 * time.Millisecond
	defer func() { commitAckTimeout = old }()

	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	consumeUntilDelivered(t, c)

	// Swallow KV writes: delete the bucket and answer nothing on its subjects.
	if err := js.DeleteKeyValue(ctx, kvBucketName); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	sub, err := js.Conn().Subscribe("$KV."+kvBucketName+".>", func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	errc := make(chan error, 1)
	go func() { errc <- c.CommitSync(context.Background()) }()
	select {
	case err := <-errc:
		if !errors.Is(err, jetstream.ErrAsyncPublishTimeout) {
			t.Fatalf("expected ack timeout, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CommitSync hung on a lost ack")
	}
}

// recreateStream deletes the test stream, recreates it empty, and publishes n messages.
func recreateStream(t *testing.T, js jetstream.JetStream, n int) {
	t.Helper()
	ctx := context.Background()
	if err := js.DeleteStream(ctx, testStream); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: testStream, Subjects: []string{"test.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := js.Publish(ctx, "test.again", []byte("again")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
}

// A stored offset from a previous incarnation of the stream is reset.
func TestStreamRecreatedAtStartup(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	c, err := NewConsumer(ctx, js, ConsumerConfig{Stream: testStream, ConsumerName: name})
	if err != nil {
		t.Fatal(err)
	}
	consumeUntilDelivered(t, c)
	waitUntil(5*time.Second, func() bool { return lastDelivered(c) == numMessages })
	if err := c.CommitSync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Close()

	recreateStream(t, js, numMessages+20) // passes the old position: only the stream identity tells
	var reported atomic.Bool
	c2 := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name, ErrorHandler: func(_ OffsetConsumer, err error) {
		if errors.Is(err, ErrStreamRecreated) {
			reported.Store(true)
		}
	}})
	if err := c2.Consume(func(jetstream.Msg) {}); err != nil { // the claim detects it
		t.Fatal(err)
	}
	if !reported.Load() {
		t.Fatal("ErrStreamRecreated not reported")
	}
	if got := c2.(*offsetConsumer).startSeq; got != 0 {
		t.Fatalf("expected offset reset to 0, got %d", got)
	}
}

// Legacy entries (no stream identity) past the stream's end are reset.
func TestStreamRecreatedLegacyEntry(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	kv, err := ensureBucket(ctx, js, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, testStream+"."+name, []byte(`{"seq":5000,"committed_at":"2026-01-01T00:00:00Z"}`)); err != nil {
		t.Fatal(err)
	}
	c := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name})
	if err := c.Consume(func(jetstream.Msg) {}); err != nil { // the claim resets it
		t.Fatal(err)
	}
	if got := c.(*offsetConsumer).startSeq; got != 0 {
		t.Fatalf("expected legacy offset beyond the stream end to be reset, got %d", got)
	}
}

// A stream deleted and recreated while consuming is detected through the
// ordered consumer reset, and consumption restarts from the beginning.
func TestStreamRecreatedWhileConsuming(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())

	var reported atomic.Bool
	var seqs atomic.Int64
	c := newTestConsumer(t, js, ConsumerConfig{ErrorHandler: func(_ OffsetConsumer, err error) {
		if errors.Is(err, ErrStreamRecreated) {
			reported.Store(true)
		}
	}})
	var afterReset atomic.Int64
	if err := c.Consume(func(msg jetstream.Msg) {
		seqs.Add(1)
		if reported.Load() {
			afterReset.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c) == numMessages }) {
		t.Fatal("initial messages not delivered")
	}

	recreateStream(t, js, 10)
	if !waitUntil(20*time.Second, func() bool { return reported.Load() }) {
		t.Fatal("stream recreation not detected")
	}
	if !waitUntil(10*time.Second, func() bool { return afterReset.Load() >= 10 }) {
		t.Fatalf("expected the 10 messages of the new stream, got %d", afterReset.Load())
	}
}

// Several instances starting to consume at the same time: exactly one wins
// the claim, and only its ordered consumer exists.
func TestConcurrentStarters(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	type result struct {
		c   OffsetConsumer
		err error
	}
	results := make(chan result, 4)
	for i := 0; i < 4; i++ {
		go func() {
			c, err := NewConsumer(context.Background(), connect(t, srv.ClientURL()), ConsumerConfig{Stream: testStream, ConsumerName: name})
			if err == nil {
				if err = c.Consume(func(jetstream.Msg) {}); err != nil {
					c.Close()
				}
			}
			results <- result{c, err}
		}()
	}
	var winners []OffsetConsumer
	for i := 0; i < 4; i++ {
		r := <-results
		if r.err == nil {
			winners = append(winners, r.c)
		} else if !errors.Is(r.err, ErrAlreadyInUse) {
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("expected exactly one winner, got %d", len(winners))
	}
	defer winners[0].Close()
	if names := consumerNamesWithPrefix(t, js, "oc-"+testStream+"-"+name+"-"); len(names) != 1 ||
		!strings.HasPrefix(names[0], winners[0].(*offsetConsumer).owner) {
		t.Fatalf("expected only the winner's ordered consumer, got %v", names)
	}
}

// Close does not release an entry that another instance claimed meanwhile
// (an instance displaced without noticing): the entry is left untouched and
// Close reports ErrAlreadyInUse.
func TestCloseSkipsReleaseOfForeignEntry(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	consumeUntilDelivered(t, c)
	impl := c.(*offsetConsumer)
	other := `{"seq":3,"committed_at":"","owner":"oc-someone-else","host":"h"}`
	if _, err := impl.kv.Put(ctx, impl.kvKey, []byte(other)); err != nil {
		t.Fatal(err)
	}

	err := c.Close()
	var inUse *AlreadyInUseError
	if !errors.Is(err, ErrAlreadyInUse) || !errors.As(err, &inUse) || inUse.Owner != "oc-someone-else" {
		t.Fatalf("expected ErrAlreadyInUse naming the new owner, got %v", err)
	}
	if info, _ := c.Info(ctx); info == nil || info.Owner != "oc-someone-else" || info.Seq != 3 {
		t.Fatalf("Close overwrote the other owner's entry: %+v", info)
	}
	if names := consumerNamesWithPrefix(t, js, impl.owner); len(names) != 0 {
		t.Fatalf("consumers left behind after Close: %v", names)
	}
}

// Close does not recreate an entry deleted by someone else.
func TestCloseSkipsReleaseOfDeletedEntry(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	consumeUntilDelivered(t, c) // claims the entry
	impl := c.(*offsetConsumer)
	if err := impl.kv.Delete(ctx, impl.kvKey); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if info, err := c.Info(ctx); err != nil || info != nil {
		t.Fatalf("expected no entry, got %+v (%v)", info, err)
	}
}

// The conditional release still succeeds right after a burst of async
// commits whose acks may land between its read and its write.
func TestCloseReleasesAfterCommitBurst(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	if err := c.Consume(func(jetstream.Msg) {
		if _, err := c.CommitAsync(); err != nil {
			t.Errorf("CommitAsync: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(5*time.Second, func() bool { return lastDelivered(c) == numMessages }) {
		t.Fatal("messages not delivered")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	info, err := c.Info(ctx)
	if err != nil || info == nil || info.Owner != "" || info.Seq != numMessages {
		t.Fatalf("expected a released entry at %d, got %+v (%v)", numMessages, info, err)
	}
}

// withLivenessWait sets the takeover wait for one test.
func withLivenessWait(t *testing.T, d time.Duration) {
	old := livenessWait
	livenessWait = d
	t.Cleanup(func() { livenessWait = old })
}

// putOwnedEntry stores an entry owned by owner, an instance that has no
// consumer on the server.
func putOwnedEntry(t *testing.T, js jetstream.JetStream, name, owner string, seq uint64) {
	t.Helper()
	ctx := context.Background()
	kv, err := ensureBucket(ctx, js, 1)
	if err != nil {
		t.Fatal(err)
	}
	entry := fmt.Sprintf(`{"seq":%d,"committed_at":"","owner":"%s","host":"h"}`, seq, owner)
	if _, err := kv.Put(ctx, testStream+"."+name, []byte(entry)); err != nil {
		t.Fatal(err)
	}
}

// Nothing is written before the first Consume: construction, commits and
// Close leave no entry and no consumer behind.
func TestNoWritesBeforeConsume(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	if err := c.CommitSync(ctx); err != nil {
		t.Fatalf("CommitSync: %v", err)
	}
	f, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("CommitAsync: %v", err)
	}
	<-f.Ok()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if info, err := c.Info(ctx); err != nil || info != nil {
		t.Fatalf("expected no entry, got %+v (%v)", info, err)
	}
	if names := consumerNamesWithPrefix(t, js, "oc-"); len(names) != 0 {
		t.Fatalf("expected no consumers, got %v", names)
	}
}

// Two instances may be constructed while the entry is unowned; the second
// one to Consume is refused because the first one's ordered consumer exists.
func TestSecondConsumeRefused(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	a := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name})
	b := newTestConsumer(t, connect(t, srv.ClientURL()), ConsumerConfig{ConsumerName: name})
	if err := a.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume a: %v", err)
	}
	err := b.Consume(func(jetstream.Msg) {})
	var inUse *AlreadyInUseError
	if !errors.As(err, &inUse) || inUse.Owner != a.(*offsetConsumer).owner {
		t.Fatalf("expected AlreadyInUse naming a, got %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close b: %v", err)
	}
	if info, _ := a.Info(context.Background()); info == nil || info.Owner != a.(*offsetConsumer).owner {
		t.Fatalf("refused instance disturbed the entry: %+v", info)
	}
}

// An owner without a consumer is taken over only after the liveness wait,
// keeping the stored offset.
func TestTakeoverAfterLivenessWait(t *testing.T) {
	withLivenessWait(t, time.Second)
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	putOwnedEntry(t, js, name, "oc-gone-owner", 7)
	c := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name})
	start := time.Now()
	if err := c.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if elapsed := time.Since(start); elapsed < livenessWait {
		t.Fatalf("took over after %v, before the liveness wait", elapsed)
	}
	if got := c.(*offsetConsumer).startSeq; got != 7 {
		t.Fatalf("expected the stored offset 7, got %d", got)
	}
	if info, _ := c.Info(context.Background()); info == nil || info.Owner != c.(*offsetConsumer).owner {
		t.Fatalf("expected the entry claimed, got %+v", info)
	}
}

// A consumer of the owner that appears during the wait (an ordered consumer
// being recreated) makes the takeover fail.
func TestConsumerAppearingDuringWaitRefuses(t *testing.T) {
	withLivenessWait(t, time.Second)
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())
	owner := "oc-resetting-owner"

	putOwnedEntry(t, js, name, owner, 0)
	c := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name})
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = js.CreateConsumer(ctx, testStream, jetstream.ConsumerConfig{
			Name: owner + "-g1_2", InactiveThreshold: time.Minute,
		})
	}()
	if err := c.Consume(func(jetstream.Msg) {}); !errors.Is(err, ErrAlreadyInUse) {
		t.Fatalf("expected ErrAlreadyInUse, got %v", err)
	}
}

// A write to the entry during the wait (here a commit by the owner) makes
// the CAS claim fail.
func TestWriteDuringWaitRefuses(t *testing.T) {
	withLivenessWait(t, time.Second)
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())
	owner := "oc-committing-owner"

	putOwnedEntry(t, js, name, owner, 3)
	c := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name})
	go func() {
		time.Sleep(300 * time.Millisecond)
		putOwnedEntry(t, js, name, owner, 4)
	}()
	if err := c.Consume(func(jetstream.Msg) {}); !errors.Is(err, ErrAlreadyInUse) {
		t.Fatalf("expected ErrAlreadyInUse, got %v", err)
	}
	if info, _ := c.Info(context.Background()); info == nil || info.Owner != owner || info.Seq != 4 {
		t.Fatalf("expected the owner's write to stand, got %+v", info)
	}
}

// A seek creates the new ordered consumer before deleting the old one: the
// instance always has a consumer on the server.
func TestSeekKeepsConsumerVisible(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	consumeUntilDelivered(t, c)
	owner := c.(*offsetConsumer).owner

	stop := make(chan struct{})
	var gaps atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream, _ := js.Stream(ctx, testStream)
		for {
			select {
			case <-stop:
				return
			default:
			}
			alive, err := isOwnerAlive(ctx, stream, owner)
			if err == nil && !alive {
				gaps.Add(1)
			}
		}
	}()
	for i := 0; i < 20; i++ {
		if err := c.Seek(ctx, uint64(i)); err != nil {
			t.Fatalf("Seek: %v", err)
		}
	}
	close(stop)
	<-done
	if n := gaps.Load(); n != 0 {
		t.Fatalf("the instance had no consumer on the server %d times during seeks", n)
	}
	if names := consumerNamesWithPrefix(t, js, owner); len(names) != 1 || !strings.HasPrefix(names[0], owner+"-g21") {
		t.Fatalf("expected only the latest generation, got %v", names)
	}
}

// A seek before the first Consume is only recorded, and stored by the claim.
func TestSeekBeforeConsumeStoredByClaim(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()

	c := newTestConsumer(t, js, ConsumerConfig{})
	if err := c.Seek(ctx, 42); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if info, err := c.Info(ctx); err != nil || info != nil {
		t.Fatalf("expected no write before Consume, got %+v (%v)", info, err)
	}
	var first atomic.Uint64
	if err := c.Consume(func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		first.CompareAndSwap(0, md.Sequence.Stream)
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := storedSeq(t, c); got != 42 {
		t.Fatalf("expected the claim to store 42, got %d", got)
	}
	if !waitUntil(5*time.Second, func() bool { return first.Load() != 0 }) || first.Load() != 43 {
		t.Fatalf("expected delivery from 43, got %d", first.Load())
	}
}

// A stream recreated between construction and Consume is detected by the
// claim: the stored offset of the old stream is not used.
func TestStreamRecreatedBeforeConsume(t *testing.T) {
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	ctx := context.Background()
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())

	c1 := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name})
	consumeUntilDelivered(t, c1)
	waitUntil(5*time.Second, func() bool { return lastDelivered(c1) == numMessages })
	if err := c1.CommitSync(ctx); err != nil {
		t.Fatal(err)
	}
	c1.Close()

	var reported atomic.Bool
	c2 := newTestConsumer(t, js, ConsumerConfig{ConsumerName: name, ErrorHandler: func(_ OffsetConsumer, err error) {
		if errors.Is(err, ErrStreamRecreated) {
			reported.Store(true)
		}
	}})
	recreateStream(t, js, numMessages+20) // passes the old position: only the identity tells
	if err := c2.Consume(func(jetstream.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if !reported.Load() {
		t.Fatal("ErrStreamRecreated not reported")
	}
	if got := c2.(*offsetConsumer).startSeq; got != 0 {
		t.Fatalf("expected the offset reset to 0, got %d", got)
	}
}

// Several starters taking over a dead owner all wake from the liveness wait
// at the same moment (there is no jitter): the CAS against the revision read
// before the wait lets exactly one of them win.
func TestConcurrentTakeover(t *testing.T) {
	withLivenessWait(t, 500*time.Millisecond)
	srv := RunBasicJetStreamServer()
	t.Cleanup(func() { shutdownJSServerAndRemoveStorage(t, srv) })
	js := setupStream(t, srv.ClientURL())
	name := fmt.Sprintf("c-%d", time.Now().UnixNano())
	putOwnedEntry(t, js, name, "oc-dead-owner", 7)

	const n = 4
	consumers := make([]OffsetConsumer, n)
	for i := range consumers {
		consumers[i] = newTestConsumer(t, connect(t, srv.ClientURL()), ConsumerConfig{ConsumerName: name})
	}
	errs := make(chan error, n)
	for _, c := range consumers {
		go func() { errs <- c.Consume(func(jetstream.Msg) {}) }()
	}
	wins := 0
	for i := 0; i < n; i++ {
		err := <-errs
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, ErrAlreadyInUse):
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly one winner, got %d", wins)
	}
	if names := consumerNamesWithPrefix(t, js, "oc-"+testStream+"-"+name+"-"); len(names) != 1 {
		t.Fatalf("expected only the winner's ordered consumer, got %v", names)
	}
}
