package offsetconsumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// closeTimeout bounds how long Close waits for pending async commits.
// A variable so tests can shorten it.
var closeTimeout = 5 * time.Second

// opTimeout bounds the individual server operations the library issues on
// its own behalf (deleting the ordered consumer, the release write, auto-commits).
const opTimeout = 5 * time.Second

// NewConsumer creates a new offset consumer for the given stream and consumer name.
// It ensures the offset-consumers KV bucket exists, reads any stored offset,
// resolves the start sequence, and claims ownership of the offset entry.
// If another live instance owns the entry (its ordered consumer still exists
// on the server, or its claim is younger than InactiveThreshold), NewConsumer
// returns an *AlreadyInUseError (errors.Is(err, ErrAlreadyInUse)).
// It does NOT start consuming — call Consume() for that.
func NewConsumer(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig) (OffsetConsumer, error) {
	if cfg.ConsumerName == "" || strings.Contains(cfg.ConsumerName, ".") {
		return nil, ErrInvalidConsumerName
	}
	if cfg.Stream == "" {
		return nil, fmt.Errorf("offset consumer: stream name is required")
	}

	stream, err := js.Stream(ctx, cfg.Stream)
	if err != nil {
		return nil, fmt.Errorf("offset consumer: stream %q not found: %w", cfg.Stream, err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("offset consumer: failed to get stream info: %w", err)
	}

	kv, err := ensureBucket(ctx, js, info.Config.Replicas)
	if err != nil {
		return nil, err
	}

	kvKey := cfg.Stream + "." + cfg.ConsumerName
	host, _ := os.Hostname()

	c := &offsetConsumer{
		js:     js,
		config: cfg,
		stream: stream,
		kv:     kv,
		kvKey:  kvKey,
		owner:  ownerPrefix(cfg.Stream, cfg.ConsumerName),
		host:   host,
	}
	c.putSubject = kvPutSubject(ctx, js, kv, kvKey)

	// Read the stored entry and check that nobody else owns it.
	entry, revision, found, err := readEntry(ctx, kv, kvKey)
	if err != nil {
		return nil, err
	}
	if found && entry.Owner != "" {
		if err := c.checkOwnership(ctx, entry); err != nil {
			return nil, err
		}
	}

	// Claim ownership with a single CAS write that keeps the stored offset.
	claim := offsetEntry{
		Seq:         entry.Seq,
		CommittedAt: entry.CommittedAt,
		Owner:       c.owner,
		Host:        c.host,
		ClaimedAt:   rfc3339Now(),
	}
	if _, err := claimEntry(ctx, kv, kvKey, claim, revision); err != nil {
		if errors.Is(err, errClaimConflict) {
			current, _, ok, _ := readEntry(ctx, kv, kvKey)
			if !ok {
				current = entry
			}
			return nil, &AlreadyInUseError{Owner: current.Owner, Host: current.Host, ClaimedAt: current.ClaimedAt}
		}
		return nil, err
	}

	c.claimedAt = claim.ClaimedAt
	c.startSeq = entry.Seq
	c.lastDeliveredSeq = entry.Seq
	c.lastCommittedSeq = entry.Seq
	c.lastCommittedAt = entry.CommittedAt

	return c, nil
}

// ownerPrefix builds the unique owner prefix for an instance. Ordered
// consumers created by the instance get names starting with this prefix.
func ownerPrefix(stream, consumerName string) string {
	return "oc-" + stream + "-" + consumerName + "-" + nuid.Next()
}

// checkOwnership returns an *AlreadyInUseError if the entry's owner is still
// alive: either its claim is younger than the inactive threshold, or a
// consumer with its prefix still exists on the stream.
func (c *offsetConsumer) checkOwnership(ctx context.Context, entry offsetEntry) error {
	inUse := &AlreadyInUseError{Owner: entry.Owner, Host: entry.Host, ClaimedAt: entry.ClaimedAt}

	if t, err := time.Parse(time.RFC3339, entry.ClaimedAt); err == nil && time.Since(t) < c.inactiveThreshold() {
		return inUse
	}

	alive, err := isOwnerAlive(ctx, c.stream, entry.Owner)
	if err != nil {
		return err
	}
	if alive {
		return inUse
	}
	return nil
}

// entryFor builds the entry to write for a commit at seq, carrying the
// instance's ownership fields.
func (c *offsetConsumer) entryFor(seq uint64, committedAt string) offsetEntry {
	return offsetEntry{
		Seq:         seq,
		CommittedAt: committedAt,
		Owner:       c.owner,
		Host:        c.host,
		ClaimedAt:   c.claimedAt,
	}
}

// Consume starts delivering messages from the resolved start position.
func (c *offsetConsumer) Consume(handler jetstream.MessageHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrConsumerClosed
	}
	if c.consumeCtx != nil {
		return ErrAlreadyConsuming
	}

	return c.startConsuming(handler)
}

// startConsuming creates an ordered consumer and starts delivering messages.
// Must be called with c.mu held.
func (c *offsetConsumer) startConsuming(handler jetstream.MessageHandler) error {
	c.generation++
	ocfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    c.config.FilterSubjects,
		NamePrefix:        fmt.Sprintf("%s-g%d", c.owner, c.generation),
		InactiveThreshold: c.inactiveThreshold(),
	}

	if c.startSeq > 0 {
		ocfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		ocfg.OptStartSeq = c.startSeq + 1
	} else if c.config.StartPosition == StartPositionEnd {
		ocfg.DeliverPolicy = jetstream.DeliverNewPolicy
	} else {
		ocfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	cons, err := c.js.OrderedConsumer(context.Background(), c.config.Stream, ocfg)
	if err != nil {
		return fmt.Errorf("offset consumer: failed to create ordered consumer: %w", err)
	}

	var consumeOpts []jetstream.PullConsumeOpt
	if c.config.ErrorHandler != nil {
		consumeOpts = append(consumeOpts, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			if errors.Is(err, jetstream.ErrOrderedConsumerReset) {
				return
			}
			c.config.ErrorHandler(c, err)
		}))
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		md, mdErr := msg.Metadata()
		if mdErr == nil {
			c.mu.Lock()
			c.lastDeliveredSeq = md.Sequence.Stream
			c.mu.Unlock()
		}
		handler(msg)
	}, consumeOpts...)
	if err != nil {
		c.deleteOrderedConsumer(cons)
		return fmt.Errorf("offset consumer: failed to start consuming: %w", err)
	}

	c.consumeCtx = cc
	c.currentConsumer = cons
	c.currentHandler = handler

	// Start auto-commit timer if enabled
	if c.config.AutoCommit && c.autoCommitStop == nil {
		c.autoCommitStop = make(chan struct{})
		c.autoCommitDone = make(chan struct{})
		go c.runAutoCommit()
	}

	return nil
}

// detachConsumer clears and returns the active consume context and ordered
// consumer. Must be called with c.mu held.
func (c *offsetConsumer) detachConsumer() (jetstream.ConsumeContext, jetstream.Consumer) {
	cc, cons := c.consumeCtx, c.currentConsumer
	c.consumeCtx = nil
	c.currentConsumer = nil
	return cc, cons
}

// stopOrderedConsumer stops delivery and deletes the ordered consumer from
// the server so that ownership is visibly released. Must be called without
// c.mu held.
func (c *offsetConsumer) stopOrderedConsumer(cc jetstream.ConsumeContext, cons jetstream.Consumer) {
	if cc != nil {
		cc.Stop()
	}
	c.deleteOrderedConsumer(cons)
}

// deleteOrderedConsumer deletes the ordered consumer's current server-side
// consumer. Best effort: errors (including "not found") are ignored.
func (c *offsetConsumer) deleteOrderedConsumer(cons jetstream.Consumer) {
	if cons == nil {
		return
	}
	info := cons.CachedInfo()
	if info == nil || info.Name == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	_ = c.js.DeleteConsumer(ctx, c.config.Stream, info.Name)
}

// runAutoCommit runs the auto-commit timer loop in a background goroutine.
func (c *offsetConsumer) runAutoCommit() {
	defer close(c.autoCommitDone)

	ticker := time.NewTicker(c.autoCommitInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			needsCommit := !c.closed && c.lastDeliveredSeq > c.lastCommittedSeq
			c.mu.Unlock()

			if needsCommit {
				ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
				err := c.CommitSync(ctx)
				cancel()
				if err != nil && !errors.Is(err, ErrConsumerClosed) && c.config.ErrorHandler != nil {
					c.config.ErrorHandler(c, fmt.Errorf("auto-commit failed: %w", err))
				}
			}
		case <-c.autoCommitStop:
			return
		}
	}
}

// CommitSync persists the last delivered sequence number synchronously.
// It is an async commit followed by a wait for its acknowledgement, so sync
// and async commits share one ordered pipeline.
func (c *offsetConsumer) CommitSync(ctx context.Context) error {
	f, err := c.CommitAsync()
	if err != nil {
		return err
	}
	select {
	case <-f.Ok():
		return nil
	case err := <-f.Err():
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CommitAsync persists the last delivered sequence number asynchronously.
// The KV write is published directly (no round trip) and the returned future
// completes when the server acknowledges it. Never blocks on the server.
func (c *offsetConsumer) CommitAsync() (jetstream.PubAckFuture, error) {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConsumerClosed
	}
	seq := c.lastDeliveredSeq
	epoch := c.commitEpoch
	if seq == 0 {
		c.mu.Unlock()
		return newCompletedFuture(), nil
	}
	at := rfc3339Now()
	if seq > c.lastRequestedSeq {
		c.lastRequestedSeq, c.lastRequestedAt = seq, at
	}
	// Registered under mu so that Close (which sets closed under mu before
	// waiting) always sees this commit.
	c.inflight.Add(1)
	c.mu.Unlock()

	data, err := marshalEntry(c.entryFor(seq, at))
	if err != nil {
		c.inflight.Done()
		return nil, err
	}

	inner, err := c.js.PublishAsync(c.putSubject, data)
	if err != nil {
		c.inflight.Done()
		return nil, fmt.Errorf("offset consumer: async KV write failed: %w", err)
	}

	f := newCommitFuture(inner)
	go c.trackCommit(inner, f, seq, at, epoch)
	return f, nil
}

// trackCommit waits for the ack of an async commit, records the committed
// sequence, and then completes the caller's future.
func (c *offsetConsumer) trackCommit(inner jetstream.PubAckFuture, f *commitFuture, seq uint64, at string, epoch uint64) {
	defer c.inflight.Done()
	select {
	case ack := <-inner.Ok():
		c.mu.Lock()
		if epoch == c.commitEpoch && seq > c.lastCommittedSeq {
			c.lastCommittedSeq = seq
			c.lastCommittedAt = at
		}
		c.mu.Unlock()
		f.ok <- ack
	case err := <-inner.Err():
		f.errCh <- fmt.Errorf("offset consumer: async KV write failed: %w", err)
	}
}

// waitInflight waits for pending async commits, bounded by timeout.
func (c *offsetConsumer) waitInflight(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		c.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// Info performs a fresh read of the consumer's offset entry from the KV store.
// Returns nil if the entry does not exist.
func (c *offsetConsumer) Info(ctx context.Context) (*OffsetConsumerInfo, error) {
	entry, _, found, err := readEntry(ctx, c.kv, c.kvKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	info := entry.info()
	return &info, nil
}

// Close stops consumption and releases all resources.
func (c *offsetConsumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if c.autoCommitStop != nil {
		close(c.autoCommitStop)
	}
	autoCommitDone := c.autoCommitDone
	c.mu.Unlock()

	// Wait for the auto-commit goroutine and for pending async commits.
	if autoCommitDone != nil {
		<-autoCommitDone
	}
	c.waitInflight(closeTimeout)

	// Stop delivery and delete the ordered consumer.
	c.mu.Lock()
	cc, cons := c.detachConsumer()
	c.mu.Unlock()
	c.stopOrderedConsumer(cc, cons)

	// Release ownership with one final write. It persists the highest
	// position handed to a commit call (acknowledged or not: publishes still
	// in flight were sent before this write, so it supersedes them) and, with
	// AutoCommit, any uncommitted messages.
	c.mu.Lock()
	seq, at := c.lastCommittedSeq, c.lastCommittedAt
	if c.lastRequestedSeq > seq {
		seq, at = c.lastRequestedSeq, c.lastRequestedAt
	}
	finalCommit := c.config.AutoCommit && c.lastDeliveredSeq > seq
	if finalCommit {
		seq, at = c.lastDeliveredSeq, rfc3339Now()
	}
	deleted := c.kvDeleted
	c.mu.Unlock()
	if deleted {
		return nil
	}

	data, err := marshalEntry(offsetEntry{Seq: seq, CommittedAt: at})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	c.commitMu.Lock()
	_, err = c.kv.Put(ctx, c.kvKey, data)
	c.commitMu.Unlock()
	if err != nil {
		return fmt.Errorf("offset consumer: failed to release offset entry: %w", err)
	}

	c.mu.Lock()
	if seq > c.lastCommittedSeq {
		c.lastCommittedSeq, c.lastCommittedAt = seq, at
	}
	c.mu.Unlock()
	return nil
}

// commitFuture is the future returned by CommitAsync. It completes after the
// consumer has recorded the acknowledged commit, so that a caller observing
// Ok() also observes the updated committed sequence.
type commitFuture struct {
	inner jetstream.PubAckFuture
	ok    chan *jetstream.PubAck
	errCh chan error
}

func newCommitFuture(inner jetstream.PubAckFuture) *commitFuture {
	return &commitFuture{
		inner: inner,
		ok:    make(chan *jetstream.PubAck, 1),
		errCh: make(chan error, 1),
	}
}

func (f *commitFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *commitFuture) Err() <-chan error            { return f.errCh }
func (f *commitFuture) Msg() *nats.Msg               { return f.inner.Msg() }

// completedFuture is returned by CommitAsync when there is nothing to commit.
type completedFuture struct {
	ok    chan *jetstream.PubAck
	errCh chan error
}

func newCompletedFuture() *completedFuture {
	f := &completedFuture{
		ok:    make(chan *jetstream.PubAck, 1),
		errCh: make(chan error),
	}
	f.ok <- &jetstream.PubAck{}
	return f
}

func (f *completedFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *completedFuture) Err() <-chan error            { return f.errCh }
func (f *completedFuture) Msg() *nats.Msg               { return nil }
