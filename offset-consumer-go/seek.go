package offsetconsumer

import (
	"context"
	"fmt"
)

// Seek repositions the consumer to a specific sequence number.
// After Seek(N), the next message delivered will be sequence N+1.
// Stops current consumption, deletes its ordered consumer, writes the new
// offset, and restarts consumption under a new generation prefix if it was
// running. If the KV write fails the consumer is left stopped.
func (c *offsetConsumer) Seek(ctx context.Context, seq uint64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConsumerClosed
	}
	cc, cons := c.detachConsumer()
	handler := c.currentHandler
	c.mu.Unlock()

	c.stopOrderedConsumer(cc, cons)

	// Persist the new offset. Holding commitMu keeps the write ordered with
	// respect to in-flight commit submissions; bumping the epoch makes acks
	// of older async commits leave lastCommittedSeq alone.
	at := rfc3339Now()
	data, err := marshalEntry(c.entryFor(seq, at))
	if err != nil {
		return err
	}
	c.commitMu.Lock()
	c.mu.Lock()
	c.commitEpoch++
	c.mu.Unlock()
	_, err = c.kv.Put(ctx, c.kvKey, data)
	if err == nil {
		// Update the position before releasing commitMu so that any commit
		// submitted after the seek write commits the seek position.
		c.mu.Lock()
		c.startSeq = seq
		c.lastDeliveredSeq = seq
		c.lastCommittedSeq = seq
		c.lastCommittedAt = at
		c.lastRequestedSeq = seq
		c.lastRequestedAt = at
		c.mu.Unlock()
	}
	c.commitMu.Unlock()
	if err != nil {
		return fmt.Errorf("offset consumer: KV write failed: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if handler != nil && !c.closed {
		return c.startConsuming(handler)
	}
	return nil
}

// SeekToBeginning repositions the consumer to the beginning of the stream.
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
