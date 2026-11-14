package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Deasel011/go-cdc/data"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Dialect is the Postgres engine.
type Dialect struct{}

func (Dialect) Driver() string { return "pgx" }

func (Dialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

func (Dialect) Quote(ident string) (string, error) { return data.QuoteIdent(ident) }

func (Dialect) OutboxDDL() string {
	return `CREATE TABLE IF NOT EXISTS "cdc_outbox" (
		"id" BIGSERIAL PRIMARY KEY,
		"msg_id" TEXT NOT NULL UNIQUE,
		"payload" BYTEA NOT NULL
	)`
}

func (Dialect) AppliedDDL() string { return data.AppliedDDL }

func (Dialect) PrepareDSN(dsn string) (string, error) {
	if dsn == "" {
		return "", errDSN
	}
	return dsn, nil
}

func (Dialect) Configure(*sql.DB) {}

func (Dialect) IsRetryable(err error) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) {
		return false
	}
	// serialization_failure, deadlock_detected
	return pg.Code == "40001" || pg.Code == "40P01"
}

func (Dialect) IsConflict(err error) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23505" {
		return false
	}
	// Primary-key violations are the upsert's job. Anything else is a second unique key.
	return !strings.HasSuffix(pg.ConstraintName, "_pkey")
}

const errDSN dataError = "postgres: dsn is required"

type dataError string

func (e dataError) Error() string { return string(e) }
