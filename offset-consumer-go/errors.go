package offsetconsumer

import "errors"

var (
	// ErrCASConflict indicates the offset was modified externally since the last read/write.
	ErrCASConflict = errors.New("offset consumer: CAS conflict")

	// ErrAsyncCommitInFlight indicates CommitAsync was called while a previous commit is pending.
	ErrAsyncCommitInFlight = errors.New("offset consumer: async commit already in flight")

	// ErrConsumerClosed indicates an operation was attempted on a closed consumer.
	ErrConsumerClosed = errors.New("offset consumer: consumer is closed")

	// ErrInvalidConsumerName indicates the consumer name is empty or contains '.'.
	ErrInvalidConsumerName = errors.New("offset consumer: invalid consumer name")

	// ErrAlreadyConsuming indicates Consume was called on a consumer that is already consuming.
	ErrAlreadyConsuming = errors.New("offset consumer: already consuming")

	// ErrNotConsuming indicates an operation requiring active consumption was called on a stopped consumer.
	ErrNotConsuming = errors.New("offset consumer: not consuming")
)
