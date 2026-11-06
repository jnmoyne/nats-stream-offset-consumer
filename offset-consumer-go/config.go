package offsetconsumer

import "time"

// StartPosition controls where a new consumer (no stored offset) begins.
type StartPosition int

const (
	// StartPositionBeginning starts from the first message in the stream.
	StartPositionBeginning StartPosition = iota

	// StartPositionEnd starts from only new messages published after the consumer starts.
	StartPositionEnd
)

// ConsumerConfig is the configuration for an offset consumer.
type ConsumerConfig struct {
	// Stream is the name of the JetStream stream to consume. Required.
	Stream string

	// ConsumerName is the logical name for this consumer. Required.
	// Must not be empty. Must not contain '.'.
	// Together with Stream forms the KV key: "{Stream}.{ConsumerName}".
	ConsumerName string

	// AutoCommit enables timer-based background commits. Default: false.
	AutoCommit bool

	// AutoCommitInterval is the interval for auto-commit. Default: 5s.
	// Ignored when AutoCommit is false.
	AutoCommitInterval time.Duration

	// FilterSubjects is an optional list of subject filters for the ordered consumer.
	FilterSubjects []string

	// ErrorHandler is called when a terminal error occurs on the underlying
	// ordered consumer, or when an auto-commit fails. Receives a reference to
	// the OffsetConsumer and the error. If nil, such errors are dropped.
	ErrorHandler func(OffsetConsumer, error)

	// StartPosition controls where a new consumer (no stored offset) begins.
	// Default: StartPositionBeginning (sequence 1).
	StartPosition StartPosition

	// InactiveThreshold is the server-side inactivity threshold of the
	// underlying ephemeral ordered consumer. After an instance crashes, its
	// ordered consumer (and with it the ownership of the offset entry) is
	// released by the server once this threshold elapses. It is also the
	// grace window during which a fresh ownership claim is considered alive
	// even if its ordered consumer has not been created yet. Default: 30s.
	InactiveThreshold time.Duration
}

const (
	defaultAutoCommitInterval = 5 * time.Second
	defaultInactiveThreshold  = 30 * time.Second
)
