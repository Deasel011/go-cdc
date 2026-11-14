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

// Change is one committed row change. Seq is a per-origin monotonic
// sequence (the source outbox id). Appliers drop a change whose Seq is
// not newer than the last applied Seq for the same origin, table, and key,
// so a redelivery after the JetStream dedup window cannot roll a row backward.
type Change struct {
	ID     string  `json:"id"`
	Origin string  `json:"origin"`
	Seq    int64   `json:"seq"`
	Op     Op      `json:"op"`
	Table  string  `json:"table"`
	Key    []Field `json:"key"`
	After  []Field `json:"after,omitzero"`
}

// Field is one column in a key or an after-image.
type Field struct {
	Name  string `json:"name"`
	Value Value  `json:"value"`
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

// Validate checks the change before it is applied. Identifiers are restricted
// to a letter or underscore followed by letters, digits, or underscores.
func (c Change) Validate() error {
	if c.ID == "" || c.Origin == "" {
		return ErrEventIdentity
	}
	switch c.Op {
	case Insert, Update, Delete:
	default:
		return errWrap(ErrUnsupportedOp, string(c.Op))
	}
	if err := ValidIdent(c.Table); err != nil {
		return err
	}
	if c.Table == "cdc_outbox" || c.Table == "cdc_applied" {
		return ErrReservedTable
	}
	if len(c.Key) == 0 {
		return ErrEventKey
	}
	seenKey := make(map[string]bool, len(c.Key))
	for _, f := range c.Key {
		if err := checkField(f, seenKey); err != nil {
			return err
		}
	}
	if c.Op == Delete {
		if len(c.After) != 0 {
			return ErrDeleteAfter
		}
		return nil
	}
	if len(c.After) == 0 {
		return ErrMissingAfter
	}
	seenAfter := make(map[string]bool, len(c.After))
	for _, f := range c.After {
		if err := checkField(f, seenAfter); err != nil {
			return err
		}
	}
	switch c.Op {
	case Insert:
		for _, k := range c.Key {
			if !seenAfter[k.Name] {
				return errWrap(ErrMissingKeyColumn, k.Name)
			}
		}
	case Update:
		// The key may be repeated in the after-image when the image is a full row.
		// A different value would change the primary key, which apply cannot track.
		keyVal := make(map[string]Value, len(c.Key))
		for _, k := range c.Key {
			keyVal[k.Name] = k.Value
		}
		for _, f := range c.After {
			kv, ok := keyVal[f.Name]
			if ok && (kv.Type != f.Value.Type || kv.Data != f.Value.Data) {
				return ErrPrimaryKeyUpdate
			}
		}
	}
	return nil
}

func checkField(f Field, seen map[string]bool) error {
	if err := ValidIdent(f.Name); err != nil {
		return err
	}
	if seen[f.Name] {
		return errWrap(ErrDuplicateColumn, f.Name)
	}
	seen[f.Name] = true
	if _, err := f.Value.Bind(); err != nil {
		return err
	}
	return nil
}

// CompleteAfter reports whether After contains every key column, so an
// applier can upsert the row when an update matches nothing.
func (c Change) CompleteAfter() bool {
	have := make(map[string]bool, len(c.After))
	for _, f := range c.After {
		have[f.Name] = true
	}
	for _, k := range c.Key {
		if !have[k.Name] {
			return false
		}
	}
	return len(c.Key) > 0
}
