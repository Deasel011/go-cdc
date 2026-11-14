package cdc

import "fmt"

// Error is a sentinel returned by this package. Values are consts so callers
// can use errors.Is, including on errors that wrap the sentinel with %w.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrInvalidIdentifier Error = "cdc: invalid identifier"
	ErrUnsupportedOp     Error = "cdc: unsupported op"
	ErrDuplicateColumn   Error = "cdc: duplicate column"
	ErrMissingKeyColumn  Error = "cdc: missing primary key column"
	ErrPrimaryKeyUpdate  Error = "cdc: primary key updates are unsupported"
	ErrEventIdentity     Error = "cdc: event missing id or origin"
	ErrEventKey          Error = "cdc: event missing key"
	ErrMissingAfter      Error = "cdc: event missing after-image"
	ErrDeleteAfter       Error = "cdc: delete event has an after-image"
	ErrReservedTable     Error = "cdc: refusing to apply a reserved table"
	ErrBadValue          Error = "cdc: bad value"
	ErrUnknownValueType  Error = "cdc: unknown value type"
	ErrUnsupportedValue  Error = "cdc: unsupported value"
	ErrNamedArg          Error = "cdc: sql.Named arguments are unsupported"
	ErrNonFiniteFloat    Error = "cdc: non-finite float"
	ErrNullData          Error = "cdc: null value has data"
)

func errWrap(sentinel Error, detail string) error {
	if detail == "" {
		return sentinel
	}
	return fmt.Errorf("%w: %s", sentinel, detail)
}
