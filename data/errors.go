package data

import "errors"

// Error is a sentinel returned by apply. Callers use errors.Is.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrNil Error = "data: nil pool or dialect"
	// ErrConflict is a unique violation on a column that is not the primary key.
	// The row is not applied. This is a documented limitation, not a retry.
	ErrConflict Error = "data: unique conflict"
	// ErrIncompleteUpdate is an update that matched no row and whose after-image
	// does not include the key, so it cannot be turned into an upsert.
	ErrIncompleteUpdate Error = "data: update matched no rows and after-image is incomplete"
)

// retryable marks a driver error the consumer should nak with a delay.
type retryable struct{ error }

func (e retryable) Unwrap() error { return e.error }

// Retryable wraps err so stream consumers can tell a lock error from a permanent one.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return retryable{error: err}
}

// IsRetryable reports whether err was wrapped by Retryable.
func IsRetryable(err error) bool {
	_, ok := errors.AsType[retryable](err)
	return ok
}
