package offsetconsumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// closeTimeout bounds how long Close waits for pending async commits.
// A variable so tests can shorten it.
var closeTimeout = 5 * time.Second

// commitAckTimeout bounds how long an async commit waits for its ack before
// failing. A variable so tests can shorten it.
var commitAckTimeout = 5 * time.Second

// opTimeout bounds the individual server operations the library issues on
// its own behalf (deleting the ordered consumer, the release write, auto-commits).
const opTimeout = 5 * time.Second

// streamWatchInterval is how often a missing stream is polled after an
// ordered consumer error.
const streamWatchInterval = time.Second

// livenessWait is how long a starter waits, measured with its own clock,
// before taking over an entry whose owner has no consumer on the server. It
// must exceed the time a live owner can be without one: an ordered consumer
// reset deletes the old consumer before creating the new one, retrying the
// creation after 1s and 2s if it fails. No jitter is needed: concurrent
// starters are decided by the claim's CAS. A variable so tests can shorten it.
var livenessWait = 5 * time.Second

// claimRounds bounds how often a claim is retried after losing a CAS race to
// a write that did not change the owner (a release, or this instance's own
// late commit).
const claimRounds = 3

// NewConsumer creates a new offset consumer for the given stream and consumer name.
// It ensures the offset-consumers KV bucket exists and checks the stored
// offset entry; it writes nothing. If the entry's owner is visibly alive (a
// consumer whose name starts with its owner prefix exists on the stream),
// NewConsumer returns an *AlreadyInUseError (errors.Is(err, ErrAlreadyInUse)).
// Ownership is claimed when consumption starts (see Consume).
// StartPositionEnd is resolved to the stream's last sequence here; the claim
// stores it if there is no usable stored offset by then.
// It does NOT start consuming — call Consume() for that.
func NewConsumer(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig) (OffsetConsumer, error) {
	if cfg.Stream == "" {
		return nil, fmt.Errorf("%w: stream name is required", ErrInvalidStreamName)
	}
	if err := validateNames(cfg.Stream, cfg.ConsumerName); err != nil {
		return nil, err
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

	commitJS, err := jetstream.New(js.Conn(), jetstream.WithPublishAsyncTimeout(commitAckTimeout))
	if err != nil {
		return nil, fmt.Errorf("offset consumer: failed to create commit context: %w", err)
	}

	kvKey := cfg.Stream + "." + cfg.ConsumerName
	host, _ := os.Hostname()

	c := &offsetConsumer{
		js:            js,
		commitJS:      commitJS,
		config:        cfg,
		stream:        stream,
		kv:            kv,
		kvKey:         kvKey,
		owner:         ownerPrefix(cfg.Stream, cfg.ConsumerName),
		host:          host,
		streamCreated: streamCreatedID(info.Created),
	}
	c.putSubject = kvPutSubject(ctx, js, kv, kvKey)
	c.initialSeq, c.initialAt = c.initialPosition(info.State.LastSeq)

	// Fail fast if the entry is visibly owned. This check does not wait: the
	// full check runs when consumption starts.
	entry, _, found, err := readEntry(ctx, kv, kvKey)
	if err != nil {
		return nil, err
	}
	if found && entry.Owner != "" {
		alive, err := isOwnerAlive(ctx, stream, entry.Owner)
		if err != nil {
			return nil, err
		}
		if alive {
			return nil, &AlreadyInUseError{Owner: entry.Owner, Host: entry.Host}
		}
	}
	return c, nil
}

// claim takes ownership of the offset entry before consumption starts, and
// stores the position consumption starts from: the pending seek position if
// Seek was called while not consuming, else the stored offset, else the
// position resolved from StartPosition at construction (also when the stored
// offset belongs to an earlier incarnation of the stream).
//
// The stored owner, if it is another instance, is alive while a consumer
// whose name starts with its owner prefix exists. If none exists, the claim
// waits livenessWait (measured locally) and lists again, which
// rides out an ordered consumer reset; it then writes with a CAS against the
// revision read before the wait, so any write during the wait (a commit by the
// owner, another instance's claim) makes it fail. No clocks are compared.
// Must be called with c.lifeMu held and c.mu not held.
func (c *offsetConsumer) claim() error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout+livenessWait+opTimeout)
	defer cancel()

	for round := 1; ; round++ {
		entry, revision, found, err := readEntry(ctx, c.kv, c.kvKey)
		if err != nil {
			return err
		}
		if found && entry.Owner != "" && entry.Owner != c.owner {
			if err := c.checkOwnership(ctx, entry); err != nil {
				return err
			}
		}

		info, err := c.stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("offset consumer: failed to get stream info: %w", err)
		}
		c.mu.Lock()
		if current := streamCreatedID(info.Created); current != "" && current != c.streamCreated {
			// The stream was recreated since construction: the start
			// position resolved then refers to the old stream.
			c.streamCreated = current
			c.initialSeq, c.initialAt = c.initialPosition(info.State.LastSeq)
		}
		recreated := found && streamWasRecreated(entry, c.streamCreated, info.State.LastSeq)
		seq, committedAt := entry.Seq, entry.CommittedAt
		switch {
		case c.pendingSeek:
			seq, committedAt = c.startSeq, c.lastCommittedAt
		case !found || recreated:
			seq, committedAt = c.initialSeq, c.initialAt
		}
		claim := offsetEntry{
			Seq:           seq,
			CommittedAt:   committedAt,
			Owner:         c.owner,
			Host:          c.host,
			StreamCreated: c.streamCreated,
		}
		c.mu.Unlock()

		rev := revision
		if !found {
			rev = 0
		}
		c.commitMu.Lock()
		_, err = claimEntry(ctx, c.kv, c.kvKey, claim, rev)
		if err == nil {
			c.mu.Lock()
			c.claimed = true
			c.pendingSeek = false
			c.kvDeleted = false
			c.commitEpoch++
			c.startSeq = seq
			c.lastDeliveredSeq = seq
			c.lastProcessedSeq = seq
			c.lastCommittedSeq = seq
			c.lastCommittedAt = committedAt
			c.lastRequestedSeq = seq
			c.lastRequestedAt = committedAt
			c.mu.Unlock()
		}
		c.commitMu.Unlock()
		if err == nil {
			if recreated {
				c.reportError(fmt.Errorf("%w (stored sequence %d)", ErrStreamRecreated, entry.Seq))
			}
			return nil
		}
		if !errors.Is(err, errClaimConflict) {
			return err
		}
		// Someone wrote the entry meanwhile. Another owner just proved it
		// is alive; a release (empty owner) or this instance's own late
		// commit is retried.
		current, _, ok, rerr := readEntry(ctx, c.kv, c.kvKey)
		if rerr != nil {
			return rerr
		}
		if ok && current.Owner != "" && current.Owner != c.owner {
			return &AlreadyInUseError{Owner: current.Owner, Host: current.Host}
		}
		if round == claimRounds {
			return fmt.Errorf("offset consumer: failed to claim offset entry: %w", err)
		}
	}
}

// initialPosition returns the position to start from when there is no usable
// stored offset: 0 (deliver all) for StartPositionBeginning, the stream's
// last sequence for StartPositionEnd.
func (c *offsetConsumer) initialPosition(lastSeq uint64) (uint64, string) {
	if c.config.StartPosition == StartPositionEnd && lastSeq > 0 {
		return lastSeq, rfc3339Now()
	}
	return 0, ""
}

// reportError passes err to the configured ErrorHandler, if any.
func (c *offsetConsumer) reportError(err error) {
	if c.config.ErrorHandler != nil {
		c.config.ErrorHandler(c, err)
	}
}

// ownerPrefix builds the unique owner prefix for an instance. Ordered
// consumers created by the instance get names starting with this prefix.
func ownerPrefix(stream, consumerName string) string {
	return "oc-" + stream + "-" + consumerName + "-" + nuid.Next()
}

// checkOwnership returns an *AlreadyInUseError if the entry's owner (another
// instance) is alive: a consumer whose name starts with its owner prefix
// exists on the stream, now or after livenessWait.
func (c *offsetConsumer) checkOwnership(ctx context.Context, entry offsetEntry) error {
	inUse := &AlreadyInUseError{Owner: entry.Owner, Host: entry.Host}
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(livenessWait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		alive, err := isOwnerAlive(ctx, c.stream, entry.Owner)
		if err != nil {
			return err
		}
		if alive {
			return inUse
		}
	}
	return nil
}

// entryFor builds the entry to write for a commit at seq, carrying the
// instance's ownership fields. Must be called with c.mu held.
func (c *offsetConsumer) entryFor(seq uint64, committedAt string) offsetEntry {
	return offsetEntry{
		Seq:           seq,
		CommittedAt:   committedAt,
		Owner:         c.owner,
		Host:          c.host,
		StreamCreated: c.streamCreated,
	}
}

// Consume claims ownership of the offset entry (see claim) and starts
// delivering messages from the resolved start position.
func (c *offsetConsumer) Consume(handler jetstream.MessageHandler) error {
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConsumerClosed
	}
	if c.consumeCtx != nil {
		c.mu.Unlock()
		return ErrAlreadyConsuming
	}
	c.mu.Unlock()

	// Claim (or re-validate) ownership every time consumption starts from
	// a stopped state: while not consuming, the instance has no consumer on
	// the server and another instance may have taken the entry over.
	if err := c.claim(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrConsumerClosed
	}
	return c.startConsuming(handler)
}

// startConsuming creates an ordered consumer and starts delivering messages.
// Must be called with c.lifeMu and c.mu held.
func (c *offsetConsumer) startConsuming(handler jetstream.MessageHandler) error {
	c.generation++
	gen := c.generation
	c.lastConsumerName = ""
	c.repositioning = false
	ocfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    c.config.FilterSubjects,
		NamePrefix:        fmt.Sprintf("%s-g%d", c.owner, gen),
		InactiveThreshold: c.inactiveThreshold(),
	}

	// The start position is fully resolved by NewConsumer and Seek: 0 means
	// the beginning of the stream.
	if c.startSeq > 0 {
		ocfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		ocfg.OptStartSeq = c.startSeq + 1
	} else {
		ocfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	cons, err := c.js.OrderedConsumer(context.Background(), c.config.Stream, ocfg)
	if err != nil {
		return fmt.Errorf("offset consumer: failed to create ordered consumer: %w", err)
	}

	consumeOpts := []jetstream.PullConsumeOpt{jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		// The ordered consumer is about to be (or was) recreated. If the
		// stream itself was recreated, the new ordered consumer waits at the
		// old position, possibly forever: watch for that.
		if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrNoHeartbeat) ||
			errors.Is(err, jetstream.ErrOrderedConsumerReset) || errors.Is(err, jetstream.ErrStreamNotFound) {
			c.watchStream(gen)
		}
		if errors.Is(err, jetstream.ErrOrderedConsumerReset) {
			return
		}
		c.reportError(err)
	})}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		c.deliver(gen, handler, msg)
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
		go c.runAutoCommit(c.autoCommitStop)
	}

	return nil
}

// isCurrent reports whether generation gen is the active consumption.
// Must be called with c.mu held.
func (c *offsetConsumer) isCurrent(gen int) bool {
	return c.consumeCtx != nil && c.generation == gen && !c.repositioning
}

// deliver tracks positions around one handler call. Messages from a stopped
// or replaced generation are dropped, so a seek is never undone by a late
// message of the previous ordered consumer.
func (c *offsetConsumer) deliver(gen int, handler jetstream.MessageHandler, msg jetstream.Msg) {
	md, err := msg.Metadata()
	if err != nil {
		handler(msg)
		return
	}
	seq := md.Sequence.Stream

	c.mu.Lock()
	if !c.isCurrent(gen) {
		c.mu.Unlock()
		return
	}
	reset := c.lastConsumerName != "" && md.Consumer != c.lastConsumerName
	c.lastConsumerName = md.Consumer
	c.mu.Unlock()

	// The client recreated the ordered consumer. If that is because the
	// stream was deleted and recreated, the ordered consumer silently resumes
	// at the old position on the new stream: reposition instead.
	if reset {
		if created, changed := c.streamChanged(); changed {
			c.mu.Lock()
			if !c.isCurrent(gen) {
				c.mu.Unlock()
				return
			}
			c.repositioning = true
			c.mu.Unlock()
			go c.repositionAfterRecreate(created)
			return
		}
	}

	c.mu.Lock()
	if !c.isCurrent(gen) {
		c.mu.Unlock()
		return
	}
	c.lastDeliveredSeq = seq
	c.mu.Unlock()

	handler(msg)

	c.mu.Lock()
	if c.isCurrent(gen) && seq > c.lastProcessedSeq {
		c.lastProcessedSeq = seq
	}
	c.mu.Unlock()
}

// watchStream polls the stream after an ordered consumer error until the
// stream exists again, and repositions if it is a new incarnation. At most
// one watcher runs at a time; it gives up when generation gen ends.
func (c *offsetConsumer) watchStream(gen int) {
	c.mu.Lock()
	if c.watching || !c.isCurrent(gen) {
		c.mu.Unlock()
		return
	}
	c.watching = true
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.watching = false
			c.mu.Unlock()
		}()
		for {
			c.mu.Lock()
			current := c.isCurrent(gen)
			c.mu.Unlock()
			if !current {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			info, err := c.stream.Info(ctx)
			cancel()
			if err == nil {
				created := streamCreatedID(info.Created)
				c.mu.Lock()
				changed := created != "" && c.streamCreated != "" && created != c.streamCreated && c.isCurrent(gen)
				if changed {
					c.repositioning = true
				}
				c.mu.Unlock()
				if changed {
					c.repositionAfterRecreate(created)
				}
				return
			}
			time.Sleep(streamWatchInterval)
		}
	}()
}

// streamChanged reports whether the stream's incarnation differs from the
// one the offsets refer to, and returns the current one.
func (c *offsetConsumer) streamChanged() (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	info, err := c.stream.Info(ctx)
	if err != nil {
		return "", false
	}
	created := streamCreatedID(info.Created)
	c.mu.Lock()
	defer c.mu.Unlock()
	return created, created != "" && c.streamCreated != "" && created != c.streamCreated
}

// repositionAfterRecreate moves the consumer to StartPosition on the new
// stream and reports ErrStreamRecreated.
// The new identity is applied by the seek write itself, so no commit can
// store a position of the old stream under the new stream's identity.
func (c *offsetConsumer) repositionAfterRecreate(created string) {
	c.mu.Lock()
	old := c.lastDeliveredSeq
	c.mu.Unlock()

	c.reportError(fmt.Errorf("%w (stored sequence %d)", ErrStreamRecreated, old))

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	var lastSeq uint64
	if info, err := c.stream.Info(ctx); err == nil {
		lastSeq = info.State.LastSeq
	}
	seq, _ := c.initialPosition(lastSeq)
	if err := c.seek(ctx, seq, created); err != nil && !errors.Is(err, ErrConsumerClosed) {
		c.reportError(fmt.Errorf("offset consumer: failed to reposition after stream recreation: %w", err))
	}
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
// It commits the last processed sequence, never a message whose handler is
// still running. Close does not wait for this goroutine, so the ErrorHandler
// may call Close.
func (c *offsetConsumer) runAutoCommit(stop chan struct{}) {
	ticker := time.NewTicker(c.autoCommitInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			needsCommit := !c.closed && c.lastProcessedSeq > c.lastCommittedSeq
			c.mu.Unlock()

			if needsCommit {
				ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
				err := c.awaitCommit(ctx, true)
				cancel()
				if err != nil && !errors.Is(err, ErrConsumerClosed) {
					c.reportError(fmt.Errorf("auto-commit failed: %w", err))
				}
			}
		case <-stop:
			return
		}
	}
}

// CommitSync persists the last delivered sequence number synchronously.
// It is an async commit followed by a wait for its acknowledgement, so sync
// and async commits share one ordered pipeline.
func (c *offsetConsumer) CommitSync(ctx context.Context) error {
	return c.awaitCommit(ctx, false)
}

func (c *offsetConsumer) awaitCommit(ctx context.Context, processedOnly bool) error {
	f, err := c.commitAsync(processedOnly)
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
// completes when the server acknowledges it, or fails after the commit ack
// timeout. Never blocks on the server.
func (c *offsetConsumer) CommitAsync() (jetstream.PubAckFuture, error) {
	return c.commitAsync(false)
}

// commitAsync submits a commit of the last delivered sequence, or with
// processedOnly of the last processed sequence (auto-commit).
func (c *offsetConsumer) commitAsync(processedOnly bool) (jetstream.PubAckFuture, error) {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConsumerClosed
	}
	if !c.claimed {
		// Nothing was delivered, and the entry is not ours to write yet.
		c.mu.Unlock()
		return newCompletedFuture(), nil
	}
	seq := c.lastDeliveredSeq
	if processedOnly {
		seq = c.lastProcessedSeq
		if seq <= c.lastRequestedSeq {
			// An explicit commit already covers it.
			c.mu.Unlock()
			return newCompletedFuture(), nil
		}
	}
	epoch := c.commitEpoch
	if seq == 0 {
		c.mu.Unlock()
		return newCompletedFuture(), nil
	}
	at := rfc3339Now()
	if seq > c.lastRequestedSeq {
		c.lastRequestedSeq, c.lastRequestedAt = seq, at
	}
	entry := c.entryFor(seq, at)
	// Registered under mu so that Close (which sets closed under mu before
	// waiting) always sees this commit.
	c.inflight.Add(1)
	c.mu.Unlock()

	data, err := marshalEntry(entry)
	if err != nil {
		c.inflight.Done()
		return nil, err
	}

	inner, err := c.commitJS.PublishAsync(c.putSubject, data)
	if err != nil {
		c.inflight.Done()
		return nil, fmt.Errorf("offset consumer: async KV write failed: %w", err)
	}

	f := newCommitFuture(inner)
	go c.trackCommit(inner, f, seq, at, epoch)
	return f, nil
}

// trackCommit waits for the ack of an async commit, records the committed
// sequence, and then completes the caller's future. The commit context's
// publish ack timeout guarantees that one of the two channels fires.
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
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if c.autoCommitStop != nil {
		close(c.autoCommitStop)
	}
	c.mu.Unlock()

	// Wait for pending async commits. The auto-commit goroutine is not
	// waited for: it stops on its own and any commit it submits after this
	// point fails with ErrConsumerClosed.
	c.waitInflight(closeTimeout)
	// Free the commit context's reply subscription once the remaining
	// commits settle (bounded by the commit ack timeout).
	go func() {
		c.inflight.Wait()
		c.commitJS.CleanupPublisher()
	}()

	// Stop delivery and delete the ordered consumer.
	c.mu.Lock()
	cc, cons := c.detachConsumer()
	c.mu.Unlock()
	c.stopOrderedConsumer(cc, cons)

	// Release ownership with one final conditional write (releaseEntry). It
	// persists the highest position handed to a commit call (acknowledged or
	// not: publishes still in flight were sent before this write, so it
	// supersedes them) and, with AutoCommit, the messages processed since the
	// last commit.
	c.mu.Lock()
	seq, at := c.lastCommittedSeq, c.lastCommittedAt
	if c.lastRequestedSeq > seq {
		seq, at = c.lastRequestedSeq, c.lastRequestedAt
	}
	if c.config.AutoCommit && c.lastProcessedSeq > seq {
		seq, at = c.lastProcessedSeq, rfc3339Now()
	}
	// Before the first claim the entry is not this instance's to write.
	deleted := c.kvDeleted || !c.claimed
	release := offsetEntry{Seq: seq, CommittedAt: at, StreamCreated: c.streamCreated}
	c.mu.Unlock()

	if deleted {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	c.commitMu.Lock()
	released, err := c.releaseEntry(ctx, release)
	c.commitMu.Unlock()
	if err != nil {
		return err
	}
	if !released {
		return nil
	}

	c.mu.Lock()
	if seq > c.lastCommittedSeq {
		c.lastCommittedSeq, c.lastCommittedAt = seq, at
	}
	c.mu.Unlock()
	return nil
}

// releaseAttempts bounds how often Close re-reads the entry when its
// conditional release write loses a race.
const releaseAttempts = 3

// releaseEntry writes the release only while the entry still names this
// instance as its owner, with a compare-and-set against the revision just
// read. An instance that was displaced without noticing (its ordered
// consumer expired and another instance claimed the entry) therefore cannot
// clear the new owner's claim, which would let a third instance in without a
// liveness check. A revision conflict means a write landed between the read
// and the release, most likely one of this instance's own late async
// commits, so the entry is read again. It returns released=false, without
// error, when the entry no longer exists (nothing to release), and an
// *AlreadyInUseError when another instance owns it.
func (c *offsetConsumer) releaseEntry(ctx context.Context, release offsetEntry) (released bool, err error) {
	for attempt := 1; ; attempt++ {
		current, revision, found, err := readEntry(ctx, c.kv, c.kvKey)
		if err != nil {
			return false, fmt.Errorf("offset consumer: failed to release offset entry: %w", err)
		}
		if !found {
			return false, nil
		}
		if current.Owner != c.owner {
			return false, fmt.Errorf("offset consumer: release skipped, the entry is no longer owned by this instance: %w",
				&AlreadyInUseError{Owner: current.Owner, Host: current.Host})
		}
		_, err = claimEntry(ctx, c.kv, c.kvKey, release, revision)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, errClaimConflict) || attempt == releaseAttempts {
			return false, fmt.Errorf("offset consumer: failed to release offset entry: %w", err)
		}
	}
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
