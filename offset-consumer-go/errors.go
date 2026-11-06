package offsetconsumer

import (
	"errors"
	"fmt"
)

var (
	// ErrConsumerClosed indicates an operation was attempted on a closed consumer.
	ErrConsumerClosed = errors.New("offset consumer: consumer is closed")

	// ErrInvalidConsumerName indicates the consumer name is empty, too long,
	// or contains characters other than letters, digits, '-', '_' and '='.
	ErrInvalidConsumerName = errors.New("offset consumer: invalid consumer name")

	// ErrInvalidStreamName indicates the stream name cannot be used in the
	// offset entry's KV key or the ordered consumer names: it is empty or
	// contains characters other than letters, digits, '-', '_' and '='.
	ErrInvalidStreamName = errors.New("offset consumer: invalid stream name")

	// ErrStreamRecreated is reported to the ErrorHandler when the stream was
	// deleted and recreated since the offset was stored (detected at startup
	// or while consuming). The stored offset refers to the old stream, so the
	// consumer repositions itself according to StartPosition, like Kafka's
	// auto.offset.reset. It is informational: consumption continues.
	ErrStreamRecreated = errors.New("offset consumer: stream was recreated, offset reset to the start position")

	// ErrAlreadyConsuming indicates Consume was called on a consumer that is already consuming.
	ErrAlreadyConsuming = errors.New("offset consumer: already consuming")

	// ErrNotConsuming indicates an operation requiring active consumption was called on a stopped consumer.
	ErrNotConsuming = errors.New("offset consumer: not consuming")

	// ErrAlreadyInUse indicates that another live instance currently owns the
	// offset entry for this stream and consumer name. Use errors.As with
	// *AlreadyInUseError to get details about the current owner.
	ErrAlreadyInUse = errors.New("offset consumer: already in use by another instance")
)

// AlreadyInUseError is returned by NewConsumer and Consume when the offset
// entry is owned by another live instance, and by Close when the entry was
// claimed by another instance meanwhile (the release is then skipped). It
// matches ErrAlreadyInUse with errors.Is.
type AlreadyInUseError struct {
	// Owner is the owner prefix recorded in the KV entry. Ordered consumers
	// created by the owning instance have names starting with this prefix.
	Owner string
	// Host is the hostname recorded by the owning instance.
	Host string
}

func (e *AlreadyInUseError) Error() string {
	return fmt.Sprintf("%v (owner %q on host %q)", ErrAlreadyInUse, e.Owner, e.Host)
}

// Is reports whether target is ErrAlreadyInUse.
func (e *AlreadyInUseError) Is(target error) bool {
	return target == ErrAlreadyInUse
}
