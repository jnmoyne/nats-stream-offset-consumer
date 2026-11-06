package offsetconsumer

import (
	"errors"
	"fmt"
)

var (
	// ErrConsumerClosed indicates an operation was attempted on a closed consumer.
	ErrConsumerClosed = errors.New("offset consumer: consumer is closed")

	// ErrInvalidConsumerName indicates the consumer name is empty or contains '.'.
	ErrInvalidConsumerName = errors.New("offset consumer: invalid consumer name")

	// ErrAlreadyConsuming indicates Consume was called on a consumer that is already consuming.
	ErrAlreadyConsuming = errors.New("offset consumer: already consuming")

	// ErrNotConsuming indicates an operation requiring active consumption was called on a stopped consumer.
	ErrNotConsuming = errors.New("offset consumer: not consuming")

	// ErrAlreadyInUse indicates that another live instance currently owns the
	// offset entry for this stream and consumer name. Use errors.As with
	// *AlreadyInUseError to get details about the current owner.
	ErrAlreadyInUse = errors.New("offset consumer: already in use by another instance")
)

// AlreadyInUseError is returned by NewConsumer when the offset entry is owned
// by another live instance. It matches ErrAlreadyInUse with errors.Is.
type AlreadyInUseError struct {
	// Owner is the owner prefix recorded in the KV entry. Ordered consumers
	// created by the owning instance have names starting with this prefix.
	Owner string
	// Host is the hostname recorded by the owning instance.
	Host string
	// ClaimedAt is the RFC3339 timestamp at which the owner claimed the entry.
	ClaimedAt string
}

func (e *AlreadyInUseError) Error() string {
	return fmt.Sprintf("%v (owner %q on host %q, claimed at %s)", ErrAlreadyInUse, e.Owner, e.Host, e.ClaimedAt)
}

// Is reports whether target is ErrAlreadyInUse.
func (e *AlreadyInUseError) Is(target error) bool {
	return target == ErrAlreadyInUse
}
