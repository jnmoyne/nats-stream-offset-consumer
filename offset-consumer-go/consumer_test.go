package offsetconsumer

import (
	"context"
	"fmt"
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
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
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

	// Wait for messages to arrive
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= numMessages {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

	// Wait for some messages
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if lastSeq.Load() >= 10 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if gotFirst.Load() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	resumeSeq := firstResumeSeq.Load()
	t.Logf("Phase 2: first message after resume at seq %d (expected %d)", resumeSeq, committedSeq+1)

	if resumeSeq != committedSeq+1 {
		t.Errorf("expected resume at seq %d, got %d", committedSeq+1, resumeSeq)
	}

	c2.DeleteConsumer(ctx)
}

// Test 3: CommitAsync
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

	var count atomic.Int64
	err = c.Consume(func(msg jetstream.Msg) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	f, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("CommitAsync: %v", err)
	}

	select {
	case <-f.Ok():
		t.Logf("CommitAsync succeeded, %d messages consumed", count.Load())
	case err := <-f.Err():
		t.Fatalf("CommitAsync failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("CommitAsync timed out")
	}

	// Second async commit while none in flight should work
	_, err = c.CommitAsync()
	if err != nil {
		t.Fatalf("second CommitAsync should succeed: %v", err)
	}
}

// Test 4: ErrAsyncCommitInFlight
func TestAsyncCommitInFlight(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	js := setupStream(t, srv.ClientURL())
	consumerName := fmt.Sprintf("test-inflight-%d", time.Now().UnixNano())

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

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		impl := c.(*offsetConsumer)
		impl.mu.Lock()
		seq := impl.lastDeliveredSeq
		impl.mu.Unlock()
		if seq > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// First async commit
	f1, err := c.CommitAsync()
	if err != nil {
		t.Fatalf("first CommitAsync: %v", err)
	}

	// Immediately try second — should fail
	_, err = c.CommitAsync()
	if err != ErrAsyncCommitInFlight {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Log("first commit completed before second was called")
	} else {
		t.Log("correctly got ErrAsyncCommitInFlight")
	}

	select {
	case <-f1.Ok():
	case e := <-f1.Err():
		t.Fatalf("first commit error: %v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// Test 5: AutoCommit mode
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

	// Wait for messages and auto-commit to fire
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= numMessages {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

// Test 6: Seek
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

	err = c1.Consume(func(msg jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		impl := c1.(*offsetConsumer)
		impl.mu.Lock()
		seq := impl.lastDeliveredSeq
		impl.mu.Unlock()
		if seq > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if gotFirst.Load() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	seq := firstSeq.Load()
	t.Logf("First message after Seek(50): seq %d (expected 51)", seq)
	if seq != 51 {
		t.Errorf("expected seq 51 after Seek(50), got %d", seq)
	}
}

// Test 7: SeekToBeginning
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

	// Consume and commit at some position
	err = c.Consume(func(msg jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		impl := c.(*offsetConsumer)
		impl.mu.Lock()
		seq := impl.lastDeliveredSeq
		impl.mu.Unlock()
		if seq > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.CommitSync(ctx)

	// SeekToBeginning
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

// Test 8: DeleteConsumer
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

	err = c1.Consume(func(msg jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		impl := c1.(*offsetConsumer)
		impl.mu.Lock()
		seq := impl.lastDeliveredSeq
		impl.mu.Unlock()
		if seq > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

// Test 9: Invalid consumer name
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

// Test 10: StartPosition End
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

// Test 11: ErrAlreadyConsuming
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
