# Go API Contract: Offset Consumer Library

**Package**: `offsetconsumer`
**Module**: `github.com/synadia-io/offset-consumer-go`
**Client**: `github.com/nats-io/nats.go` v1.55.0 (Go 1.26+)

---

## Construction

```go
// NewConsumer creates a new offset consumer for the given stream and consumer name.
// It ensures the offset-consumers KV bucket exists, reads any stored offset,
// resolves the start sequence, and claims ownership of the offset entry.
// If another live instance owns the entry (its ordered consumer still exists
// on the server, or its claim is younger than InactiveThreshold), NewConsumer
// returns an *AlreadyInUseError (errors.Is(err, ErrAlreadyInUse)).
// It does NOT start consuming — call Consume() for that.
//
// Returns ErrInvalidConsumerName if config.ConsumerName is empty or contains '.'.
func NewConsumer(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig) (OffsetConsumer, error)

// ListConsumers returns all offset consumers for the given stream (KV keys "{stream}.*").
func ListConsumers(ctx context.Context, js jetstream.JetStream, stream string) (map[string]OffsetConsumerInfo, error)
```

---

## OffsetConsumer Interface

```go
type OffsetConsumer interface {
    // Consume starts delivering messages from the resolved start position.
    // The handler is called serially for each message.
    Consume(handler jetstream.MessageHandler) error

    // CommitSync persists the last delivered sequence number and waits for the ack.
    CommitSync(ctx context.Context) error

    // CommitAsync persists the last delivered sequence number asynchronously
    // and never blocks on the server. Commits are pipelined and applied in call
    // order. The future completes when the KV write is acknowledged; it is never
    // nil (an already-completed future when there is nothing to commit).
    CommitAsync() (jetstream.PubAckFuture, error)

    // Info performs a fresh read of the consumer's offset entry. Returns nil if
    // the entry does not exist.
    Info(ctx context.Context) (*OffsetConsumerInfo, error)

    // Seek repositions the consumer: stops consumption, deletes its ordered
    // consumer, writes the new offset, restarts under a new generation prefix
    // if it was consuming. After Seek(N) the next message is N+1. If the write
    // fails the consumer is left stopped with its position unchanged.
    Seek(ctx context.Context, seq uint64) error
    SeekToBeginning(ctx context.Context) error // Seek(0)
    SeekToEnd(ctx context.Context) error       // Seek(stream last seq)

    // DeleteConsumer removes the offset entry. Does not stop consumption; a
    // later Close() will not recreate the entry.
    DeleteConsumer(ctx context.Context) error

    // Close waits (5s bound) for pending async commits, deletes the ordered
    // consumer, and releases ownership with one final write that persists the
    // highest sequence passed to a commit call (unacknowledged commits are not
    // lost) and, with AutoCommit, the last delivered sequence.
    Close() error
}

type OffsetConsumerInfo struct {
    Seq         uint64
    CommittedAt string // RFC3339
    Owner       string // owner prefix of the live instance, "" when unowned
    Host        string
    ClaimedAt   string // RFC3339
}
```

---

## ConsumerConfig

```go
type ConsumerConfig struct {
    Stream             string
    ConsumerName       string        // non-empty, no '.'
    AutoCommit         bool          // default false
    AutoCommitInterval time.Duration // default 5s
    FilterSubjects     []string
    ErrorHandler       func(OffsetConsumer, error) // terminal consumer errors and auto-commit failures
    StartPosition      StartPosition // StartPositionBeginning (default) or StartPositionEnd
    InactiveThreshold  time.Duration // ordered consumer inactive threshold and claim grace window; default 30s
}
```

---

## Errors

```go
var (
    ErrConsumerClosed      = errors.New("offset consumer: consumer is closed")
    ErrInvalidConsumerName = errors.New("offset consumer: invalid consumer name")
    ErrAlreadyConsuming    = errors.New("offset consumer: already consuming")
    ErrNotConsuming        = errors.New("offset consumer: not consuming")
    ErrAlreadyInUse        = errors.New("offset consumer: already in use by another instance")
)

// AlreadyInUseError matches ErrAlreadyInUse with errors.Is and carries the owner details.
type AlreadyInUseError struct {
    Owner     string
    Host      string
    ClaimedAt string
}
```

Ordered consumers created by an instance are named `oc-{stream}-{consumer}-{nuid}-g{generation}_{serial}`.
