# Go API Contract: Offset Consumer Library

**Package**: `offsetconsumer`
**Module**: `github.com/synadia-io/offset-consumer-go`
**Client**: `github.com/nats-io/nats.go` v1.55.0 (Go 1.26+)

---

## Construction

```go
// NewConsumer creates a new offset consumer for the given stream and consumer name.
// It ensures the offset-consumers KV bucket exists and checks the stored
// entry; it writes nothing. If the stored owner has a consumer whose name
// starts with its owner prefix, NewConsumer returns an *AlreadyInUseError
// (errors.Is(err, ErrAlreadyInUse)). StartPositionEnd is resolved to the
// stream's last sequence here; the claim at Consume stores it when there is no
// usable stored offset. It does NOT start consuming — call Consume() for that.
//
// Returns an error matching ErrInvalidConsumerName or ErrInvalidStreamName
// unless both names match ^[A-Za-z0-9_=-]+$ and total at most 196 characters.
func NewConsumer(ctx context.Context, js jetstream.JetStream, cfg ConsumerConfig) (OffsetConsumer, error)

// ListConsumers returns all offset consumers for the given stream (KV keys "{stream}.*").
func ListConsumers(ctx context.Context, js jetstream.JetStream, stream string) (map[string]OffsetConsumerInfo, error)
```

---

## OffsetConsumer Interface

```go
type OffsetConsumer interface {
    // Consume claims the entry (CAS; if the stored owner is another instance
    // without a consumer, after a 5s wait and a second look) and starts
    // delivering messages from the resolved start position. Returns an
    // *AlreadyInUseError if another live instance owns the entry. If the
    // stored offset belongs to an earlier incarnation of the stream, it is
    // reset to StartPosition and ErrStreamRecreated is passed to the
    // ErrorHandler. The handler is called serially for each message.
    Consume(handler jetstream.MessageHandler) error

    // CommitSync persists the last delivered sequence number and waits for the
    // ack. From inside the handler it includes the current message.
    CommitSync(ctx context.Context) error

    // CommitAsync persists the last delivered sequence number asynchronously
    // and never blocks on the server. Commits are pipelined and applied in call
    // order. The future completes when the KV write is acknowledged, or fails
    // with jetstream.ErrAsyncPublishTimeout after 5s without an ack; it is never
    // nil (an already-completed future when there is nothing to commit).
    CommitAsync() (jetstream.PubAckFuture, error)

    // Info performs a fresh read of the consumer's offset entry. Returns nil if
    // the entry does not exist.
    Info(ctx context.Context) (*OffsetConsumerInfo, error)

    // Seek repositions the consumer. While consuming: stops delivery, writes
    // the new offset, starts the next generation's ordered consumer, then
    // deletes the previous one (make before break). While not consuming: only
    // records the position, which the claim at Consume stores. After Seek(N)
    // the next message is N+1. If the write fails the consumer is left stopped
    // with its position unchanged.
    Seek(ctx context.Context, seq uint64) error
    SeekToBeginning(ctx context.Context) error // Seek(0): the beginning, whatever StartPosition
    SeekToEnd(ctx context.Context) error       // Seek(stream last seq)

    // DeleteConsumer removes the offset entry. Does not stop consumption; a
    // later Close() will not recreate the entry.
    DeleteConsumer(ctx context.Context) error

    // Close waits (5s bound) for pending async commits, deletes the ordered
    // consumer, and releases ownership with one final write that persists the
    // highest sequence passed to a commit call (unacknowledged commits are not
    // lost) and, with AutoCommit, the last processed sequence (a message still
    // in its handler is not committed). Does not wait for the auto-commit
    // goroutine, so the ErrorHandler may call Close. Writes nothing if the
    // instance never claimed the entry.
    Close() error
}

type OffsetConsumerInfo struct {
    Seq           uint64
    CommittedAt   string // RFC3339
    Owner         string // owner prefix of the live instance, "" when unowned
    Host          string
    StreamCreated string // stream creation time, decimal Unix nanoseconds; "" for old entries
}

// Consume, Seek and Close are serialized per instance. Auto-commit commits the
// last processed sequence (handler returned), never a message in its handler.
```

---

## ConsumerConfig

```go
type ConsumerConfig struct {
    Stream             string
    ConsumerName       string        // ^[A-Za-z0-9_=-]+$; with Stream at most 196 characters
    AutoCommit         bool          // default false
    AutoCommitInterval time.Duration // default 5s
    FilterSubjects     []string
    ErrorHandler       func(OffsetConsumer, error) // consumer errors, auto-commit failures, ErrStreamRecreated
    StartPosition      StartPosition // StartPositionBeginning (default) or StartPositionEnd (resolved at construction)
    InactiveThreshold  time.Duration // ordered consumer inactive threshold (crash-recovery delay, plus the 5s liveness wait); default 30s
}
```

---

## Errors

```go
var (
    ErrConsumerClosed      = errors.New("offset consumer: consumer is closed")
    ErrInvalidConsumerName = errors.New("offset consumer: invalid consumer name")
    ErrInvalidStreamName   = errors.New("offset consumer: invalid stream name")
    // Informational, passed to the ErrorHandler: the offset was reset because
    // the stream was deleted and recreated. Consumption continues.
    ErrStreamRecreated     = errors.New("offset consumer: stream was recreated, offset reset to the start position")
    ErrAlreadyConsuming    = errors.New("offset consumer: already consuming")
    ErrNotConsuming        = errors.New("offset consumer: not consuming")
    ErrAlreadyInUse        = errors.New("offset consumer: already in use by another instance")
)

// AlreadyInUseError matches ErrAlreadyInUse with errors.Is and carries the owner details.
type AlreadyInUseError struct {
    Owner string
    Host  string
}
```

An instance's ordered consumers are named `oc-{stream}-{consumer}-{nuid}-g{generation}_{serial}`. A starter treats the stored owner as alive while its claim is younger than `InactiveThreshold` or a consumer with its prefix exists.

`Close()` releases with a CAS on an entry that still names the instance (at most three attempts when its own late commits race it). If another instance owns the entry, nothing is written and `Close` returns an error matching `ErrAlreadyInUse` (`errors.As` gives the `*AlreadyInUseError` of the new owner); if the entry no longer exists, nothing is written and `Close` returns nil.
