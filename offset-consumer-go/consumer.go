package offsetconsumer

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// OffsetConsumerInfo contains the stored offset information for a consumer,
// as read from the KV store.
type OffsetConsumerInfo struct {
	// Seq is the last committed stream sequence number.
	Seq uint64
	// CommittedAt is the timestamp of the last commit (RFC3339).
	CommittedAt string
	// Owner is the owner prefix of the instance currently holding the entry.
	// Empty when no instance owns it. Ordered consumers created by the owner
	// have names starting with this prefix.
	Owner string
	// Host is the hostname of the owning instance.
	Host string
	// ClaimedAt is the timestamp at which the owner claimed the entry (RFC3339).
	ClaimedAt string
}

// OffsetConsumer provides Kafka-style offset-based consumption on a NATS JetStream stream.
// All methods are safe for concurrent use from multiple goroutines.
type OffsetConsumer interface {
	// Consume starts delivering messages from the resolved start position.
	// The handler is called serially for each message.
	Consume(handler jetstream.MessageHandler) error

	// CommitSync persists the last delivered sequence number synchronously.
	CommitSync(ctx context.Context) error

	// CommitAsync persists the last delivered sequence number asynchronously
	// and never blocks on the server. Commits are pipelined: several may be in
	// flight at once and they are applied in call order. The returned future
	// completes when the KV write is acknowledged; it is never nil (when there
	// is nothing to commit an already-completed future is returned).
	CommitAsync() (jetstream.PubAckFuture, error)

	// Info performs a fresh read of the consumer's offset entry from the KV store.
	// Returns nil if the entry does not exist.
	Info(ctx context.Context) (*OffsetConsumerInfo, error)

	// Seek repositions the consumer to a specific sequence number.
	// After Seek(N), the next message delivered will be sequence N+1.
	Seek(ctx context.Context, seq uint64) error

	// SeekToBeginning repositions the consumer to the beginning of the stream.
	SeekToBeginning(ctx context.Context) error

	// SeekToEnd repositions the consumer to the current end of the stream.
	SeekToEnd(ctx context.Context) error

	// DeleteConsumer removes the offset entry from the KV store.
	DeleteConsumer(ctx context.Context) error

	// Close stops consumption and releases all resources: it waits for
	// pending async commits (bounded, 5s), deletes the underlying ordered
	// consumer, and releases ownership of the offset entry with one final
	// write. That write persists the highest sequence passed to a commit call
	// (so commits still unacknowledged after the wait are not lost) and, with
	// AutoCommit enabled, the last delivered sequence.
	Close() error
}

// offsetConsumer is the unexported implementation of OffsetConsumer.
type offsetConsumer struct {
	js     jetstream.JetStream
	config ConsumerConfig
	stream jetstream.Stream

	kv         jetstream.KeyValue
	kvKey      string
	putSubject string

	// Ownership identity, immutable after NewConsumer.
	owner     string
	host      string
	claimedAt string

	startSeq         uint64
	lastDeliveredSeq uint64
	lastCommittedSeq uint64
	lastCommittedAt  string
	// lastRequestedSeq is the highest sequence handed to a commit call; the
	// release write on Close persists at least this position even if the
	// commit's acknowledgement never arrived.
	lastRequestedSeq uint64
	lastRequestedAt  string

	// generation counts consumption (re)starts; each one gets its own
	// ordered consumer name prefix "{owner}-g{generation}".
	generation      int
	consumeCtx      jetstream.ConsumeContext
	currentConsumer jetstream.Consumer
	currentHandler  jetstream.MessageHandler
	autoCommitStop  chan struct{}
	autoCommitDone  chan struct{}

	mu        sync.Mutex
	closed    bool
	kvDeleted bool

	// commitMu serializes the submission of KV writes so that commits and
	// seeks reach the server in program order. Lock order: commitMu, then mu.
	commitMu sync.Mutex
	// commitEpoch is bumped by Seek so that acks of older async commits do
	// not move lastCommittedSeq past the seek position.
	commitEpoch uint64
	// inflight tracks async commits whose acks have not arrived yet.
	inflight sync.WaitGroup
}

// autoCommitInterval returns the configured auto-commit interval or the default.
func (c *offsetConsumer) autoCommitInterval() time.Duration {
	if c.config.AutoCommitInterval > 0 {
		return c.config.AutoCommitInterval
	}
	return defaultAutoCommitInterval
}

// inactiveThreshold returns the configured inactive threshold or the default.
func (c *offsetConsumer) inactiveThreshold() time.Duration {
	if c.config.InactiveThreshold > 0 {
		return c.config.InactiveThreshold
	}
	return defaultInactiveThreshold
}
