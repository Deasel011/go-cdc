package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
	query, args, err := Rebind(c.SQL, s.d, c.Args)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		if s.d.IsConflict(err) {
			return fmt.Errorf("%w: %s: %v", ErrConflict, c.Origin, err)
		}
		if s.d.IsRetryable(err) {
			return Retryable(err)
		}
		return err
	}
	if c.Op != cdc.Update && c.Op != cdc.Delete {
		return nil
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// The statement is the one the source ran. Matching nothing here is not
		// a reason to stop the consumer; the caller terminates this message.
		return fmt.Errorf("%w: %s", ErrIncompleteUpdate, c.ID)
	}
	return nil
}

func (s *SQL) lastSeq(ctx context.Context, tx *sql.Tx, c cdc.Change) (int64, error) {
	q := fmt.Sprintf(`SELECT "seq" FROM "cdc_applied" WHERE "origin" = %s`, s.d.Placeholder(1))
	var seq int64
	err := tx.QueryRowContext(ctx, q, c.Origin).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *SQL) saveSeq(ctx context.Context, tx *sql.Tx, c cdc.Change) error {
	q := fmt.Sprintf(`INSERT INTO "cdc_applied" ("origin", "seq") VALUES (%s, %s)
		ON CONFLICT ("origin") DO UPDATE SET "seq" = EXCLUDED."seq"
		WHERE "cdc_applied"."seq" < EXCLUDED."seq"`,
		s.d.Placeholder(1), s.d.Placeholder(2))
	_, err := tx.ExecContext(ctx, q, c.Origin, c.Seq)
	return err
}
