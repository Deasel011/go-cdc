package data

import "database/sql"

// Dialect is everything that varies between SQL engines.
// Adding an engine means implementing Dialect in one package and importing
// its driver there. Capture, apply, and JetStream do not change.
type Dialect interface {
	// Driver is the database/sql driver name, already registered by the dialect package.
	Driver() string
	// Placeholder is the nth bound parameter, 1-based.
	Placeholder(n int) string
	// Quote quotes an identifier. Names that fail cdc.ValidIdent are rejected.
	Quote(ident string) (string, error)
	OutboxDDL() string
	AppliedDDL() string
	// PrepareDSN returns the DSN to open. It may append engine defaults.
	PrepareDSN(dsn string) (string, error)
	// Configure tunes a pool after open (for example SQLite connection limits).
	Configure(db *sql.DB)
	// IsRetryable reports a lock or serialization error that should be retried.
	IsRetryable(err error) bool
	// IsConflict reports a unique violation that is not the primary key.
	// Primary-key conflicts are handled by the upsert and are not conflicts.
	IsConflict(err error) bool
}

// AppliedDDL is the bookkeeping table shared by engines that accept this SQL.
const AppliedDDL = `CREATE TABLE IF NOT EXISTS "cdc_applied" (
	"origin" TEXT NOT NULL,
	"tbl" TEXT NOT NULL,
	"key" TEXT NOT NULL,
	"seq" BIGINT NOT NULL,
	PRIMARY KEY ("origin", "tbl", "key")
)`
