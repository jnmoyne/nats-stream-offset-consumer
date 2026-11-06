package offsetconsumer

import (
	"context"
	"fmt"
)

// Seek repositions the consumer to a specific sequence number.
// After Seek(N), the next message delivered will be sequence N+1.
// While consuming, it stops delivery, writes the new offset, starts a new
// ordered consumer under the next generation prefix, and only then deletes
// the previous one, so the instance stays visible on the server throughout.
// If the KV write fails the consumer is left stopped. While not consuming,
// the position is only recorded: the claim at the next Consume stores it.
func (c *offsetConsumer) Seek(ctx context.Context, seq uint64) error {
	return c.seek(ctx, seq, "")
}

// seek implements Seek. A non-empty streamCreated switches the consumer to a
// new stream incarnation atomically with the seek write: commits submitted
// before it carry the old identity, commits after it the new one.
func (c *offsetConsumer) seek(ctx context.Context, seq uint64, streamCreated string) error {
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()

	at := rfc3339Now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConsumerClosed
	}
	if c.consumeCtx == nil {
		// Not consuming: no consumer protects the entry, so do not write it.
		c.startSeq = seq
		c.lastDeliveredSeq = seq
		c.lastProcessedSeq = seq
		c.lastCommittedAt = at
		c.pendingSeek = true
		if streamCreated != "" {
			c.streamCreated = streamCreated
		}
		c.mu.Unlock()
		return nil
	}
	cc, old := c.detachConsumer()
	handler := c.currentHandler
	c.mu.Unlock()

	// Stop delivery but keep the server-side consumer until the new one
	// exists (make before break).
	cc.Stop()

	// Persist the new offset. Holding commitMu keeps the write ordered with
	// respect to in-flight commit submissions; bumping the epoch makes acks
	// of older async commits leave lastCommittedSeq alone.
	c.commitMu.Lock()
	c.mu.Lock()
	c.commitEpoch++
	entry := c.entryFor(seq, at)
	if streamCreated != "" {
		entry.StreamCreated = streamCreated
	}
	data, err := marshalEntry(entry)
	c.mu.Unlock()
	if err == nil {
		_, err = c.kv.Put(ctx, c.kvKey, data)
		if err != nil {
			err = fmt.Errorf("offset consumer: KV write failed: %w", err)
		}
	}
	if err == nil {
		// Update the position before releasing commitMu so that any commit
		// submitted after the seek write commits the seek position.
		c.mu.Lock()
		c.startSeq = seq
		c.lastDeliveredSeq = seq
		c.lastProcessedSeq = seq
		c.lastCommittedSeq = seq
		c.lastCommittedAt = at
		c.lastRequestedSeq = seq
		c.lastRequestedAt = at
		c.kvDeleted = false // the seek write recreated the entry
		if streamCreated != "" {
			c.streamCreated = streamCreated
		}
		c.mu.Unlock()
	}
	c.commitMu.Unlock()

	if err == nil {
		c.mu.Lock()
		// lifeMu guarantees nothing else started consuming meanwhile; the
		// consumeCtx check is a second line of defence against a leaked consumer.
		if !c.closed && c.consumeCtx == nil {
			err = c.startConsuming(handler)
		}
		c.mu.Unlock()
	}
	// The new generation is visible (or the seek failed): delete the old one.
	c.deleteOrderedConsumer(old)
	return err
}

// SeekToBeginning repositions the consumer to the beginning of the stream,
// regardless of StartPosition.
func (c *offsetConsumer) SeekToBeginning(ctx context.Context) error {
	return c.Seek(ctx, 0)
}

// SeekToEnd repositions the consumer to the current end of the stream.
func (c *offsetConsumer) SeekToEnd(ctx context.Context) error {
	info, err := c.stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("offset consumer: failed to get stream info: %w", err)
	}
	return c.Seek(ctx, info.State.LastSeq)
}

// DeleteConsumer removes the offset entry from the KV store.
// Does not stop consumption — call Close() separately. A later Close() will
// not recreate the entry.
func (c *offsetConsumer) DeleteConsumer(ctx context.Context) error {
	c.commitMu.Lock()
	err := c.kv.Delete(ctx, c.kvKey)
	c.commitMu.Unlock()
	if err != nil {
		return fmt.Errorf("offset consumer: failed to delete offset entry: %w", err)
	}
	c.mu.Lock()
	c.kvDeleted = true
	c.mu.Unlock()
	return nil
}
