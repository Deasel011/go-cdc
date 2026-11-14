package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Deasel011/go-cdc"
)

// SQL applies changes to a database/sql pool using Dialect.
type SQL struct {
	db *sql.DB
	d  Dialect
}

// Open prepares the applied-sequence table and returns a Data implementation.
func Open(ctx context.Context, db *sql.DB, d Dialect) (*SQL, error) {
	if db == nil || d == nil {
		return nil, ErrNil
	}
	if _, err := db.ExecContext(ctx, d.AppliedDDL()); err != nil {
		return nil, err
	}
	return &SQL{db: db, d: d}, nil
}

// Apply writes one change. A change whose Seq is not newer than the last
// applied sequence for the same origin, table, and key is skipped.
func (s *SQL) Apply(ctx context.Context, c cdc.Change) error {
	if err := c.Validate(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if c.Seq > 0 {
		last, err := s.lastSeq(ctx, tx, c)
		if err != nil {
			return err
		}
		if c.Seq <= last {
			return nil
		}
	}
	if err := s.exec(ctx, tx, c); err != nil {
		return err
	}
	if c.Seq > 0 {
		if err := s.saveSeq(ctx, tx, c); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQL) exec(ctx context.Context, tx *sql.Tx, c cdc.Change) error {
	query, args, err := s.statement(c)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		if s.d.IsConflict(err) {
			return fmt.Errorf("%w: %s.%s: %v", ErrConflict, c.Origin, c.Table, err)
		}
		if s.d.IsRetryable(err) {
			return Retryable(err)
		}
		return err
	}
	if c.Op != cdc.Update {
		return nil
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	if n > 1 {
		return fmt.Errorf("%w: %s matched %d rows", ErrConflict, c.Table, n)
	}
	// Matched nothing. A full after-image can be upserted; a partial one cannot.
	if !c.CompleteAfter() {
		return fmt.Errorf("%w: %s", ErrIncompleteUpdate, c.Table)
	}
	upsert, args, err := s.upsertFrom(c)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, upsert, args...); err != nil {
		if s.d.IsConflict(err) {
			return fmt.Errorf("%w: %s.%s: %v", ErrConflict, c.Origin, c.Table, err)
		}
		if s.d.IsRetryable(err) {
			return Retryable(err)
		}
		return err
	}
	return nil
}

func (s *SQL) statement(c cdc.Change) (string, []any, error) {
	switch c.Op {
	case cdc.Insert:
		return s.upsertFrom(c)
	case cdc.Update:
		return s.update(c)
	case cdc.Delete:
		return s.del(c)
	default:
		return "", nil, fmt.Errorf("%w: %q", cdc.ErrUnsupportedOp, c.Op)
	}
}

func (s *SQL) upsertFrom(c cdc.Change) (string, []any, error) {
	cols := make([]string, len(c.After))
	args := make([]any, len(c.After))
	ph := make([]string, len(c.After))
	for i, f := range c.After {
		cols[i] = f.Name
		v, err := f.Value.Bind()
		if err != nil {
			return "", nil, err
		}
		args[i] = v
		ph[i] = s.d.Placeholder(i + 1)
	}
	keys := make([]string, len(c.Key))
	for i, f := range c.Key {
		keys[i] = f.Name
	}
	q, err := Upsert(c.Table, cols, keys, ph)
	if err != nil {
		return "", nil, err
	}
	return q, args, nil
}

func (s *SQL) update(c cdc.Change) (string, []any, error) {
	table, err := s.d.Quote(c.Table)
	if err != nil {
		return "", nil, err
	}
	sets := make([]string, len(c.After))
	args := make([]any, 0, len(c.After)+len(c.Key))
	for i, f := range c.After {
		q, err := s.d.Quote(f.Name)
		if err != nil {
			return "", nil, err
		}
		v, err := f.Value.Bind()
		if err != nil {
			return "", nil, err
		}
		sets[i] = q + " = " + s.d.Placeholder(len(args)+1)
		args = append(args, v)
	}
	wheres := make([]string, 0, len(c.Key))
	for _, f := range c.Key {
		q, err := s.d.Quote(f.Name)
		if err != nil {
			return "", nil, err
		}
		v, err := f.Value.Bind()
		if err != nil {
			return "", nil, err
		}
		wheres = append(wheres, q+" = "+s.d.Placeholder(len(args)+1))
		args = append(args, v)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(sets, ", "), strings.Join(wheres, " AND ")), args, nil
}

func (s *SQL) del(c cdc.Change) (string, []any, error) {
	table, err := s.d.Quote(c.Table)
	if err != nil {
		return "", nil, err
	}
	wheres := make([]string, len(c.Key))
	args := make([]any, len(c.Key))
	for i, f := range c.Key {
		q, err := s.d.Quote(f.Name)
		if err != nil {
			return "", nil, err
		}
		v, err := f.Value.Bind()
		if err != nil {
			return "", nil, err
		}
		wheres[i] = q + " = " + s.d.Placeholder(i+1)
		args[i] = v
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(wheres, " AND ")), args, nil
}

func (s *SQL) lastSeq(ctx context.Context, tx *sql.Tx, c cdc.Change) (int64, error) {
	key, err := keyText(c.Key)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(`SELECT "seq" FROM "cdc_applied" WHERE "origin" = %s AND "tbl" = %s AND "key" = %s`,
		s.d.Placeholder(1), s.d.Placeholder(2), s.d.Placeholder(3))
	var seq int64
	err = tx.QueryRowContext(ctx, q, c.Origin, c.Table, key).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *SQL) saveSeq(ctx context.Context, tx *sql.Tx, c cdc.Change) error {
	key, err := keyText(c.Key)
	if err != nil {
		return err
	}
	// Same shape on Postgres and SQLite. The WHERE keeps a newer seq from moving backward.
	q := fmt.Sprintf(`INSERT INTO "cdc_applied" ("origin", "tbl", "key", "seq") VALUES (%s, %s, %s, %s)
		ON CONFLICT ("origin", "tbl", "key") DO UPDATE SET "seq" = EXCLUDED."seq"
		WHERE "cdc_applied"."seq" < EXCLUDED."seq"`,
		s.d.Placeholder(1), s.d.Placeholder(2), s.d.Placeholder(3), s.d.Placeholder(4))
	_, err = tx.ExecContext(ctx, q, c.Origin, c.Table, key, c.Seq)
	return err
}

func keyText(key []cdc.Field) (string, error) {
	b, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
