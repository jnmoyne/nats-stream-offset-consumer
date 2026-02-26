package offsetconsumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// NewConsumer creates a new offset consumer for the given stream and consumer name.
// It ensures the offset-consumers KV bucket exists, reads any stored offset,
// and resolves the start sequence. It does NOT start consuming — call Consume() for that.
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

	replicas := info.Config.Replicas
	kv, err := ensureBucket(ctx, js, replicas)
	if err != nil {
		return nil, err
	}

	kvKey := cfg.Stream + "." + cfg.ConsumerName

	c := &offsetConsumer{
		js:     js,
		config: cfg,
		stream: stream,
		kv:     kv,
		kvKey:  kvKey,
	}

	// Read stored offset
	entry, err := kv.Get(ctx, kvKey)
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("offset consumer: failed to read stored offset: %w", err)
		}
		// No stored offset — use configured start position
		c.lastRevision = 0
		c.startSeq = 0
	} else {
		offset, err := unmarshalOffset(entry.Value())
		if err != nil {
			return nil, fmt.Errorf("offset consumer: failed to parse stored offset: %w", err)
		}
		c.lastRevision = entry.Revision()
		c.startSeq = offset.Seq
		c.lastCommittedSeq = offset.Seq
		c.lastDeliveredSeq = offset.Seq
	}

	return c, nil
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
	ocfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: c.config.FilterSubjects,
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
			// Filter out informational resets
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
		return fmt.Errorf("offset consumer: failed to start consuming: %w", err)
	}

	c.consumeCtx = cc
	c.currentHandler = handler

	// Start auto-commit timer if enabled
	if c.config.AutoCommit && c.autoCommitStop == nil {
		c.autoCommitStop = make(chan struct{})
		c.autoCommitDone = make(chan struct{})
		go c.runAutoCommit()
	}

	return nil
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
			needsCommit := c.lastDeliveredSeq > c.lastCommittedSeq
			c.mu.Unlock()

			if needsCommit {
				_ = c.commitSync(context.Background(), false)
			}
		case <-c.autoCommitStop:
			return
		}
	}
}

// CommitSync persists the last delivered sequence number synchronously.
func (c *offsetConsumer) CommitSync(ctx context.Context) error {
	return c.commitSync(ctx, false)
}

// commitSync is the internal implementation that optionally skips the closed check
// (needed for final commit during Close).
func (c *offsetConsumer) commitSync(ctx context.Context, allowClosed bool) error {
	c.mu.Lock()
	if c.closed && !allowClosed {
		c.mu.Unlock()
		return ErrConsumerClosed
	}
	seq := c.lastDeliveredSeq
	lastRev := c.lastRevision
	c.mu.Unlock()

	if seq == 0 {
		return nil // nothing to commit
	}

	data, err := marshalOffset(seq)
	if err != nil {
		return fmt.Errorf("offset consumer: failed to marshal offset: %w", err)
	}

	newRev, err := kvUpdate(ctx, c.kv, c.kvKey, data, lastRev)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.lastRevision = newRev
	c.lastCommittedSeq = seq
	c.mu.Unlock()

	return nil
}

// CommitAsync persists the last delivered sequence number asynchronously.
func (c *offsetConsumer) CommitAsync() (CommitFuture, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConsumerClosed
	}
	if c.asyncCommitInFlight {
		c.mu.Unlock()
		return nil, ErrAsyncCommitInFlight
	}
	seq := c.lastDeliveredSeq
	lastRev := c.lastRevision
	c.asyncCommitInFlight = true
	c.mu.Unlock()

	f := newCommitFuture()

	if seq == 0 {
		c.mu.Lock()
		c.asyncCommitInFlight = false
		c.mu.Unlock()
		close(f.okCh)
		return f, nil
	}

	go func() {
		defer func() {
			c.mu.Lock()
			c.asyncCommitInFlight = false
			c.mu.Unlock()
		}()

		data, err := marshalOffset(seq)
		if err != nil {
			f.errCh <- fmt.Errorf("offset consumer: failed to marshal offset: %w", err)
			return
		}

		newRev, err := kvUpdate(context.Background(), c.kv, c.kvKey, data, lastRev)
		if err != nil {
			f.errCh <- err
			return
		}

		c.mu.Lock()
		c.lastRevision = newRev
		c.lastCommittedSeq = seq
		c.mu.Unlock()

		close(f.okCh)
	}()

	return f, nil
}

// Close stops consumption and releases all resources.
// If AutoCommit is enabled and uncommitted messages exist, performs a final CommitSync.
// If AutoCommit is disabled, does NOT commit on close.
func (c *offsetConsumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	// Stop auto-commit timer if running
	if c.autoCommitStop != nil {
		close(c.autoCommitStop)
	}
	c.mu.Unlock()

	// Wait for auto-commit goroutine to finish
	if c.autoCommitDone != nil {
		<-c.autoCommitDone
	}

	// Stop the ordered consumer
	c.mu.Lock()
	cc := c.consumeCtx
	c.consumeCtx = nil
	needsFinalCommit := c.config.AutoCommit && c.lastDeliveredSeq > c.lastCommittedSeq
	c.mu.Unlock()

	if cc != nil {
		cc.Stop()
	}

	// Final commit if auto-commit is enabled and there are uncommitted messages
	if needsFinalCommit {
		return c.commitSync(context.Background(), true)
	}

	return nil
}
