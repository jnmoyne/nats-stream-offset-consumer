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
	// StreamCreated identifies the incarnation of the stream the offset
	// refers to: the stream's creation time in Unix nanoseconds, as a decimal
	// string. Empty for entries written by older versions.
	StreamCreated string
}

// OffsetConsumer provides Kafka-style offset-based consumption on a NATS JetStream stream.
// All methods are safe for concurrent use from multiple goroutines.
type OffsetConsumer interface {
	// Consume claims ownership of the offset entry and starts delivering
	// messages from the resolved start position. It returns an error matching
	// ErrAlreadyInUse if another live instance owns the entry; when the
	// stored owner has no consumer on the server, Consume first waits a few
	// seconds for it to reappear.
	// The handler is called serially for each message.
	Consume(handler jetstream.MessageHandler) error

	// CommitSync persists the last delivered sequence number synchronously.
	// Called from inside the handler, it includes the message being handled.
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
	// While consuming it writes the new offset and switches to a new ordered
	// consumer; before the first Consume it only records the position, which
	// the claim stores.
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
	// AutoCommit enabled, the last sequence whose handler has returned. A
	// message whose handler is still running is not committed by Close and is
	// redelivered on the next start. The release write is conditional: if
	// the entry no longer names this instance as its owner (another instance
	// claimed it after this one's ordered consumer expired), it is left
	// untouched and Close returns an error matching ErrAlreadyInUse; if the
	// entry no longer exists, nothing is written.
	Close() error
}

// offsetConsumer is the unexported implementation of OffsetConsumer.
type offsetConsumer struct {
	js jetstream.JetStream
	// commitJS is a JetStream context dedicated to async commits, created
	// with a publish ack timeout so that a lost ack fails the commit instead
	// of leaving it pending forever.
	commitJS jetstream.JetStream
	config   ConsumerConfig
	stream   jetstream.Stream

	kv         jetstream.KeyValue
	kvKey      string
	putSubject string

	// Ownership identity, immutable after NewConsumer.
	owner string
	host  string

	// streamCreated identifies the stream incarnation (see
	// OffsetConsumerInfo.StreamCreated). Updated when a recreation is detected.
	streamCreated string

	// claimed is set once this instance has claimed the entry (at its first
	// Consume); before that it writes nothing.
	claimed bool
	// pendingSeek is set by a Seek while not consuming: the claim at the
	// next Consume stores startSeq instead of the stored offset.
	pendingSeek bool
	// initialSeq/initialAt is the StartPosition resolved at construction,
	// used by the claim when there is no usable stored offset.
	initialSeq uint64
	initialAt  string

	startSeq         uint64
	lastDeliveredSeq uint64
	// lastProcessedSeq is the last sequence whose handler has returned.
	// Auto-commit and the final commit in Close use it, so a message still
	// being handled is never committed implicitly.
	lastProcessedSeq uint64
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
	// lastConsumerName is the server-side name of the ordered consumer that
	// delivered the last message; a change means the client reset it.
	lastConsumerName string
	// repositioning is set while a detected stream recreation is handled;
	// messages delivered meanwhile are dropped.
	repositioning bool
	// watching is set while a goroutine polls the stream for a recreation.
	watching       bool
	autoCommitStop chan struct{}

	// lifeMu serializes Consume, Seek and Close so that at most one ordered
	// consumer is ever active. Lock order: lifeMu, commitMu, mu.
	lifeMu sync.Mutex

	mu        sync.Mutex
	closed    bool
	kvDeleted bool

	// commitMu serializes the submission of KV writes so that commits and
	// seeks reach the server in program order.
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
