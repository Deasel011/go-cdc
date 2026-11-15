package cdc

import (
	"context"
	"iter"
	"regexp"
)

// Op is the kind of row change.
type Op string

const (
	Insert Op = "insert"
	Update Op = "update"
	Delete Op = "delete"
)

// Change is one committed write. SQL and Args are the statement database/sql
// already executed; apply replays them on the target. Seq is the source outbox
// id. Appliers drop a change whose Seq is not newer than the last applied Seq
// from that origin, so a redelivery after the JetStream dedup window cannot
// roll a row backward.
type Change struct {
	ID     string  `json:"id"`
	Origin string  `json:"origin"`
	Seq    int64   `json:"seq"`
	Op     Op      `json:"op"`
	SQL    string  `json:"sql"`
	Args   []Value `json:"args,omitzero"`
}

// Value is one bound parameter. Data is omitted for null.
// Integers are decimal strings so an int64 is not rounded through JSON numbers.
type Value struct {
	Type string `json:"type"`
	Data string `json:"data,omitzero"`
}

// Capture produces committed changes from a source.
// The sequence blocks until ctx is cancelled when the source is idle.
type Capture interface {
	Changes(ctx context.Context) iter.Seq2[Change, error]
}

// Data is a store changes are applied to.
type Data interface {
	Apply(ctx context.Context, c Change) error
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidIdent reports whether name can be quoted into SQL safely.
func ValidIdent(name string) error {
	if !identRe.MatchString(name) {
		return errWrap(ErrInvalidIdentifier, name)
	}
	return nil
}

// Validate checks the change before it is applied. The statement must be one
// INSERT, UPDATE, or DELETE, and Op must name that statement.
func (c Change) Validate() error {
	if c.ID == "" || c.Origin == "" {
		return ErrEventIdentity
	}
	op, err := StatementOp(c.SQL)
	if err != nil {
		return err
	}
	if c.Op != op {
		return errWrap(ErrUnsupportedOp, string(c.Op))
	}
	for _, arg := range c.Args {
		if _, err := arg.Bind(); err != nil {
			return err
		}
	}
	return nil
}
