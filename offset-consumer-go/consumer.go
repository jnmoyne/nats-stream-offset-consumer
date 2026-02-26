package offsetconsumer

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// OffsetConsumer provides Kafka-style offset-based consumption on a NATS JetStream stream.
// All methods are safe for concurrent use from multiple goroutines.
type OffsetConsumer interface {
	// Consume starts delivering messages from the resolved start position.
	// The handler is called serially for each message.
	Consume(handler jetstream.MessageHandler) error

	// CommitSync persists the last delivered sequence number synchronously.
	CommitSync(ctx context.Context) error

	// CommitAsync persists the last delivered sequence number asynchronously.
	CommitAsync() (CommitFuture, error)

	// Seek repositions the consumer to a specific sequence number.
	// After Seek(N), the next message delivered will be sequence N+1.
	Seek(ctx context.Context, seq uint64) error

	// SeekToBeginning repositions the consumer to the beginning of the stream.
	SeekToBeginning(ctx context.Context) error

	// SeekToEnd repositions the consumer to the current end of the stream.
	SeekToEnd(ctx context.Context) error

	// DeleteConsumer removes the offset entry from the KV store.
	DeleteConsumer(ctx context.Context) error

	// Close stops consumption and releases all resources.
	Close() error
}

// offsetConsumer is the unexported implementation of OffsetConsumer.
type offsetConsumer struct {
	js     jetstream.JetStream
	config ConsumerConfig
	stream jetstream.Stream

	kv          jetstream.KeyValue
	kvKey       string
	lastRevision uint64

	startSeq         uint64
	lastDeliveredSeq uint64
	lastCommittedSeq uint64

	consumeCtx          jetstream.ConsumeContext
	currentHandler      jetstream.MessageHandler
	asyncCommitInFlight bool
	autoCommitStop      chan struct{}
	autoCommitDone      chan struct{}

	mu     sync.Mutex
	closed bool
}

// autoCommitInterval returns the configured auto-commit interval or the default.
func (c *offsetConsumer) autoCommitInterval() time.Duration {
	if c.config.AutoCommitInterval > 0 {
		return c.config.AutoCommitInterval
	}
	return defaultAutoCommitInterval
}
