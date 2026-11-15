package sqlite

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/Deasel011/go-cdc/data"
	"modernc.org/sqlite"
)

// Dialect is the SQLite engine. modernc.org/sqlite is pure Go, so go test -race
// does not need cgo. mattn/go-sqlite3 is cgo and fights the race detector.
type Dialect struct{}

func (Dialect) Driver() string { return "sqlite" }

func (Dialect) Placeholder(int) string { return "?" }

func (Dialect) OutboxDDL() string {
	return `CREATE TABLE IF NOT EXISTS "cdc_outbox" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"msg_id" TEXT NOT NULL UNIQUE,
		"payload" BLOB NOT NULL
	)`
}

func (Dialect) AppliedDDL() string { return data.AppliedDDL }

// ErrMemoryDSN is returned when the DSN is an in-memory database.
// Each pooled connection would see an empty database, including an empty outbox.
const ErrMemoryDSN errorString = "sqlite: DSN must be a file database so the pool shares one database"

type errorString string

func (e errorString) Error() string { return string(e) }

func (Dialect) PrepareDSN(dsn string) (string, error) {
	if dsn == "" {
		return "", errDSN
	}
	lower := strings.ToLower(dsn)
	if strings.Contains(lower, ":memory:") || strings.Contains(lower, "mode=memory") {
		return "", ErrMemoryDSN
	}
	if !strings.Contains(dsn, "_busy_timeout=") && !strings.Contains(dsn, "_timeout=") {
		dsn = appendQuery(dsn, "_busy_timeout=5000")
	}
	if !strings.Contains(dsn, "_journal_mode=") && !strings.Contains(dsn, "_journal=") {
		dsn = appendQuery(dsn, "_journal_mode=WAL")
	}
	return dsn, nil
}

func (Dialect) Configure(db *sql.DB) { db.SetMaxOpenConns(8) }

func (Dialect) IsRetryable(err error) bool {
	e, ok := errors.AsType[*sqlite.Error](err)
	if !ok {
		return false
	}
	switch e.Code() & 0xff {
	case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
		return true
	default:
		return false
	}
}

func (Dialect) IsConflict(err error) bool {
	e, ok := errors.AsType[*sqlite.Error](err)
	if !ok {
		return false
	}
	// SQLITE_CONSTRAINT (19) with SQLITE_CONSTRAINT_UNIQUE (extended 8).
	// SQLITE_CONSTRAINT_PRIMARYKEY (extended 6) is the upsert path, not a conflict.
	if e.Code()&0xff != 19 {
		return false
	}
	ext := (e.Code() >> 8) & 0xff
	if ext == 8 {
		return true
	}
	// Some driver paths only set the primary code. The message still says which constraint it was.
	if ext == 0 && strings.Contains(e.Error(), "UNIQUE constraint failed") {
		return true
	}
	return false
}

const errDSN errorString = "sqlite: dsn is required"

func appendQuery(dsn, pair string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&" + pair
	}
	return dsn + "?" + pair
}
