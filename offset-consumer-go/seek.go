package offsetconsumer

import (
	"context"
	"fmt"
)

// Seek repositions the consumer to a specific sequence number.
// After Seek(N), the next message delivered will be sequence N+1.
// Stops current consumption, writes the new offset via CAS, and restarts.
// If the CAS write fails, returns an error and the consumer is left stopped.
func (c *offsetConsumer) Seek(ctx context.Context, seq uint64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConsumerClosed
	}

	// Stop current consumption
	cc := c.consumeCtx
	c.consumeCtx = nil
	handler := c.currentHandler
	lastRev := c.lastRevision
	c.mu.Unlock()

	if cc != nil {
		cc.Stop()
	}

	// Persist new offset
	data, err := marshalOffset(seq)
	if err != nil {
		return fmt.Errorf("offset consumer: failed to marshal offset: %w", err)
	}

	newRev, err := kvUpdate(ctx, c.kv, c.kvKey, data, lastRev)
	if err != nil {
		// Consumer is left stopped on CAS failure
		return err
	}

	// Update internal state
	c.mu.Lock()
	c.lastRevision = newRev
	c.startSeq = seq
	c.lastDeliveredSeq = seq
	c.lastCommittedSeq = seq

	// Restart consumption if we had a handler
	var restartErr error
	if handler != nil {
		restartErr = c.startConsuming(handler)
	}
	c.mu.Unlock()

	return restartErr
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
// Does not stop consumption — call Close() separately.
func (c *offsetConsumer) DeleteConsumer(ctx context.Context) error {
	err := c.kv.Delete(ctx, c.kvKey)
	if err != nil {
		return fmt.Errorf("offset consumer: failed to delete offset entry: %w", err)
	}

	c.mu.Lock()
	c.lastRevision = 0
	c.mu.Unlock()

	return nil
}
